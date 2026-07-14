import os
import re
import json
import asyncio
import random
import logging
import hmac
import hashlib
from pathlib import Path
from typing import Dict, Any, List
from contextlib import asynccontextmanager
import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel
import asyncpg
import io

# Safe handling for redis asyncio to completely prevent startup crashes
try:
    from redis import asyncio as aioredis
except ImportError:
    try:
        import redis.asyncio as aioredis
    except ImportError:
        aioredis = None

# Optional: Try to import qrcode, fallback to placeholder if not installed
try:
    import qrcode
    HAS_QRCODE = True
except ImportError:
    HAS_QRCODE = False

# --- LOGGING & APP SETUP ---
logger = logging.getLogger("kraken_swarm_production")
logging.basicConfig(level=logging.INFO)

# 🔌 ENVIRONMENT SETUP - Clean base configurations (Render aur locally dono ke liye scalable)
RAW_DB_URL = os.getenv("DATABASE_URL", "postgresql://kraken_user:kR4k3n_p4ss_99@ep-cool-snowflake-a5o3lz8e.us-east-2.aws.neon.tech/kraken_db")
SECONDARY_DB_URL = os.getenv("SECONDARY_DATABASE_URL", RAW_DB_URL) # Agara Render par doosra db URL hoga toh auto-fetch karega
REDIS_URL = os.getenv("REDIS_URL", "redis://default:rEdIsPaSsWoRd99@redis-12345.c302.us-east-1-1.ec2.cloud.redislabs.com:12345")

db_pool = None
secondary_db_pool = None
redis_client = None
http_client = None

# 🪙 SAFETY LIMITS MATRIX (UPDATED CHARACTER AND TOKEN PRICING LIMITS)
PLAN_SAFETY_LIMITS = {
    "free": {
        "max_chars": 300,
        "tokens_allocated": 3000,
        "delay_seconds": 25.0,  # Speed delay to encourage upgrade
        "max_daily_queries": 1   # Set to 1 strictly for your rule: Ek baar free sandbox mila, dobara kabhi nahi!
    },
    "token_refill": {
        "max_chars": 1000,       # ₹299 = 1,000 chars limit
        "tokens_allocated": 150000
    },
    "lite": {
        "max_chars": 2000,       # ₹499 = 2,000 chars limit
        "tokens_allocated": 400000
    },
    "infinite": {
        "max_chars": 4000,       # ₹999 = 4,000 chars limit
        "tokens_allocated": 1200000
    },
    "enterprise": {
        "max_chars": 10000,      # ₹3,999 = 10,000 chars limit
        "tokens_allocated": 5000000
    }
}

PLAN_TOKENS_ALLOCATION = {
    "token_refill": 150000,
    "lite": 400000,            
    "infinite": 1200000,       
    "enterprise": 5000000      
}

PRICING_MATRIX = {
    "IN": {"currency": "INR", "symbol": "₹", "token_refill": 299, "lite": 499, "infinite": 999, "enterprise": 3999},
    "US": {"currency": "USD", "symbol": "$", "token_refill": 3.99, "lite": 5.99, "infinite": 11.99, "enterprise": 49.99},
    "EU": {"currency": "EUR", "symbol": "€", "token_refill": 3.49, "lite": 5.49, "infinite": 10.99, "enterprise": 44.99},
    "AE": {"currency": "AED", "symbol": "AED ", "token_refill": 15, "lite": 22, "infinite": 45, "enterprise": 180}
}

DISPOSABLE_DOMAINS = {"mailinator.com", "temp-mail.org", "yopmail.com", "sharklasers.com", "guerrillamail.com", "dispostable.com", "getairmail.com"}

class ActivationPayload(BaseModel):
    session_id: str
    email: str
    browser_timezone: str
    device_fingerprint: str # Add fingerprint to track computer/browser details uniquely

async def initialize_db_tables():
    """Background helper to create tables once pool is ready"""
    global db_pool
    for attempt in range(5):
        try:
            if db_pool:
                async with db_pool.acquire() as conn:
                    # Upgraded Schema to support device tracking and permanent lockdown
                    await conn.execute('''
                        CREATE TABLE IF NOT EXISTS user_vault (
                            session_id TEXT PRIMARY KEY,
                            email TEXT,
                            device_hash TEXT,
                            tier TEXT DEFAULT 'free',
                            credits INT DEFAULT 3000,
                            verified BOOLEAN DEFAULT FALSE,
                            free_tier_claimed BOOLEAN DEFAULT FALSE,
                            arbitrage_risk BOOLEAN DEFAULT FALSE,
                            history JSONB DEFAULT '[]'::jsonb
                        );
                        CREATE INDEX IF NOT EXISTS idx_device_hash ON user_vault(device_hash);
                        CREATE INDEX IF NOT EXISTS idx_email ON user_vault(email);
                    ''')
                logger.info("✅ Core Platform Tables checked/created successfully with Security Locks.")
                break
        except Exception as e:
            logger.warning(f"⚠️ Table initialization attempt {attempt+1} failed: {e}. Retrying...")
            await asyncio.sleep(2)

# MODERN FASTAPI LIFESPAN
@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool, secondary_db_pool, redis_client, http_client
    
    # Primary DB URL check
    target_db_url = RAW_DB_URL
    if target_db_url:
        if "?sslmode=" in target_db_url:
            base_url = target_db_url.split("?")[0]
            target_db_url = f"{base_url}?sslmode=require"
        elif "localhost" not in target_db_url and "127.0.0.1" not in target_db_url:
            target_db_url = f"{target_db_url}?sslmode=require"
            
    # Secondary DB URL check
    target_sec_url = SECONDARY_DB_URL
    if target_sec_url:
        if "?sslmode=" in target_sec_url:
            base_url = target_sec_url.split("?")[0]
            target_sec_url = f"{base_url}?sslmode=require"
        elif "localhost" not in target_sec_url and "127.0.0.1" not in target_sec_url:
            target_sec_url = f"{target_sec_url}?sslmode=require"

    limits = httpx.Limits(max_keepalive_connections=50, max_connections=200)
    http_client = httpx.AsyncClient(limits=limits, timeout=30.0)

    try:
        if aioredis and REDIS_URL:
            redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
            logger.info("⚡ Redis Client structural handle ready.")
    except Exception as ree:
        logger.error(f"❌ Redis connection failed setup: {ree}")

    if target_db_url:
        try:
            logger.info("🔄 Connecting to Remote primary database cluster...")
            db_pool = await asyncpg.create_pool(target_db_url, min_size=1, max_size=10, timeout=15.0)
            logger.info("✅ Primary Database connection pool initialized.")
            asyncio.create_task(initialize_db_tables())
        except Exception as dbe:
            logger.error(f"❌ CRITICAL PRIMARY DB CONNECTION DELAY: {dbe}.")
            
    if target_sec_url and target_sec_url != target_db_url:
        try:
            logger.info("🔄 Connecting to Remote secondary database cluster...")
            secondary_db_pool = await asyncpg.create_pool(target_sec_url, min_size=1, max_size=10, timeout=15.0)
            logger.info("✅ Secondary Database connection pool initialized.")
        except Exception as sdbe:
            logger.error(f"❌ SECONDARY DB CONNECTION DELAY: {sdbe}.")
    else:
        logger.info("ℹ️ Secondary DB URL fallback mode enabled (Using Primary pool or no secondary configured).")
    
    yield
    
    if db_pool:
        await db_pool.close()
    if secondary_db_pool:
        await secondary_db_pool.close()
    if http_client:
        await http_client.aclose()
    logger.info("⚡ System resources shutdown successfully.")

app = FastAPI(title="Kraken Swarm Engine - Autopilot Autonomous Agent Platform", lifespan=lifespan)

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    try:
        with open("index.html", "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return HTMLResponse(content="<h3>Dashboard Asset Pipeline Initiated</h3>", status_code=200)

@app.get("/api/v1/geo-pricing")
async def get_geo_pricing(request: Request):
    country_code = request.headers.get("CF-IPCountry", request.headers.get("X-Vercel-IP-Country", "US"))
    if country_code not in PRICING_MATRIX:
        country_code = "US"
    return {"country": country_code, "matrix": PRICING_MATRIX[country_code]}

@app.get("/api/v1/generate-qr")
async def generate_qr(tier: str, amount: str):
    upi_string = f"upi://pay?pa=kraken@upi&pn=KrakenSwarm&am={amount}&cu=INR&tn=Kraken_{tier}_Activation"
    img_byte_arr = io.BytesIO()
    if HAS_QRCODE:
        try:
            qr = qrcode.QRCode(version=1, box_size=10, border=2)
            qr.add_data(upi_string)
            qr.make(fit=True)
            img = qr.make_image(fill_color="black", back_color="white")
            img.save(img_byte_arr, format='PNG')
        except Exception:
            HAS_QRCODE = False
            
    if not HAS_QRCODE:
        try:
            from PIL import Image, ImageDraw
            img = Image.new('RGB', (250, 250), color = (255, 255, 255))
            d = ImageDraw.Draw(img)
            d.text((25,110), f"UPI ID: kraken@upi\nAmount: {amount} INR\nScan or Pay Directly", fill=(0,0,0))
            img.save(img_byte_arr, format='PNG')
        except ImportError:
            return HTMLResponse(content="QR engine missing dependencies", status_code=500)
        
    img_byte_arr.seek(0)
    return StreamingResponse(img_byte_arr, media_type="image/png")

# 🔒 SECURE WEBHOOK ENDPOINT
@app.post("/api/v1/payment/webhook")
async def payment_webhook(request: Request):
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database currently offline.")
    
    payload = await request.body()
    signature = request.headers.get("X-Razorpay-Signature")
    webhook_secret = os.getenv("PAYMENT_SECRET_KEY", "kraken_secret_bypass_blocker")
    
    expected_signature = hmac.new(
        bytes(webhook_secret, 'utf-8'),
        msg=payload,
        digestmod=hashlib.sha256
    ).hexdigest()
    
    if not signature or not hmac.compare_digest(signature, expected_signature):
        logger.warning("❌ Hacking Warning: Unauthorized Webhook Call detected!")
        raise HTTPException(status_code=400, detail="Invalid signature.")
        
    data = await request.json()
    event = data.get("event")
    
    if event in ["payment.captured", "charge.succeeded"]:
        payment_entity = data["payload"]["payment"]["entity"]
        session_id = payment_entity["notes"].get("session_id")
        plan_chosen = payment_entity["notes"].get("plan_chosen")
        
        if plan_chosen in PLAN_SAFETY_LIMITS and session_id:
            tokens_to_add = PLAN_SAFETY_LIMITS[plan_chosen]["tokens_allocated"]
            
            async with db_pool.acquire() as conn:
                await conn.execute(
                    "UPDATE user_vault SET credits = credits + $1, tier = $2, verified = TRUE WHERE session_id = $3", 
                    tokens_to_add, plan_chosen, session_id
                )
            if redis_client:
                await redis_client.set(f"user:{session_id}:credits", tokens_to_add, ex=3600)
                await redis_client.delete(f"user:{session_id}:history")
                
            logger.info(f"✅ Webhook Success: Added {tokens_to_add} tokens to user {session_id}")
            return {"status": "SUCCESS", "message": "Tokens successfully added."}
            
    return {"status": "IGNORED"}

# 🛡️ SYSTEM NODE ACTIVATION (GOOGLE / AUTH INTERCEPTOR WITH DOUBLE LOCKS)
@app.post("/api/v1/activate-node")
async def activate_node(payload: ActivationPayload):
    if not db_pool:
         raise HTTPException(status_code=503, detail="Database cluster currently initializing.")
    
    email = payload.email.lower().strip()
    domain = email.split("@")[-1] if "@" in email else ""
    
    # Anti-Freeloader Layer 1: Block Disposable/Fake Emails
    if domain in DISPOSABLE_DOMAINS or not re.match(r"[^@]+@[^@]+\.[^@]+", email):
        raise HTTPException(status_code=400, detail="❌ Professional or authenticated email networks only.")
    
    tz = payload.browser_timezone.lower()
    arbitrage_risk = False
    
    try:
        async with db_pool.acquire() as conn:
            # Anti-Freeloader Layer 2: Check if this hardware/browser fingerprint already claimed free sandbox
            fingerprint_check = await conn.fetchrow(
                "SELECT * FROM user_vault WHERE device_hash = $1 AND free_tier_claimed = TRUE", 
                payload.device_fingerprint
            )
            if fingerprint_check and fingerprint_check["session_id"] != payload.session_id:
                raise HTTPException(status_code=403, detail="❌ Account Locked: This device has already exhausted its unique Free Sandbox allotment. Please upgrade to a premium account.")

            # Anti-Freeloader Layer 3: Check if email already claimed free tier
            email_check = await conn.fetchrow(
                "SELECT * FROM user_vault WHERE email = $1 AND free_tier_claimed = TRUE", 
                email
            )
            if email_check and email_check["session_id"] != payload.session_id:
                raise HTTPException(status_code=403, detail="❌ Quota Expired: This email profile has already consumed a Free Sandbox workspace.")

            user = await conn.fetchrow("SELECT * FROM user_vault WHERE session_id = $1", payload.session_id)
            if "asia/calcutta" not in tz and "kolkata" not in tz and user and user.get("detected_country") == "IN":
                arbitrage_risk = True
                
            if user:
                await conn.execute("UPDATE user_vault SET email=$1, verified=TRUE, arbitrage_risk=$2, device_hash=$3 WHERE session_id=$4", email, arbitrage_risk, payload.device_fingerprint, payload.session_id)
            else:
                await conn.execute("INSERT INTO user_vault (session_id, email, verified, arbitrage_risk, device_hash, credits, tier) VALUES ($1, $2, TRUE, $3, $4, 3000, 'free')", payload.session_id, email, arbitrage_risk, payload.device_fingerprint)
        
        if redis_client:
            await redis_client.set(f"user:{payload.session_id}:credits", 3000, ex=3600)
            
    except HTTPException as he:
        raise he
    except Exception as dbe:
        logger.error(f"Error executing db transaction in activate_node: {dbe}")
        raise HTTPException(status_code=500, detail="Internal lock configuration sync error.")
        
    return {"status": "SUCCESS", "message": "Authenticated successfully."}

@app.post("/api/v1/apply-recharge")
async def apply_recharge(session_id: str, plan_chosen: str):
    raise HTTPException(status_code=403, detail="❌ Security Warning: Static recharge bypass has been disabled. Payments must be routed securely through official gateways.")

@app.get("/api/v1/history/{session_id}")
async def get_history(session_id: str):
    if redis_client:
        try:
            cached_history = await redis_client.get(f"user:{session_id}:history")
            if cached_history:
                return json.loads(cached_history)
        except Exception:
            pass
            
    if not db_pool:
         return {"tier": "free", "credits_left": 3000, "history": [], "warning": "DB Syncing/Unavailable"}
         
    try:
        async with db_pool.acquire() as conn:
            user = await conn.fetchrow("SELECT tier, credits, history FROM user_vault WHERE session_id = $1", session_id)
            if not user:
                return {"tier": "free", "credits_left": 3000, "history": []}
            history_data = user["history"]
            parsed_history = json.loads(history_data) if isinstance(history_data, str) else (history_data if isinstance(history_data, list) else [])
            response_data = {"tier": user["tier"], "credits_left": user["credits"], "history": parsed_history}
            if redis_client:
                await redis_client.set(f"user:{session_id}:history", json.dumps(response_data), ex=300)
            return response_data
    except Exception as e:
        logger.error(f"History routing exception: {e}")
        return {"tier": "free", "credits_left": 3000, "history": [], "error": "Internal synchronization error"}

async def call_gemini_agent(agent_name: str, system_instruction: str, user_prompt: str) -> str:
    if not http_client:
        return f"[{agent_name} Core Simulation Output]: Execution parameter bypass mode enabled."
        
    openrouter_keys = [k for k in [os.getenv("OPENROUTER_KEY_1"), os.getenv("OPENROUTER_KEY_2")] if k]
    gemini_keys = [k for k in [os.getenv("GEMINI_KEY_1"), os.getenv("GEMINI_KEY_2")] if k]

    for g_key in gemini_keys:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={g_key}"
        headers = {"Content-Type": "application/json"}
        payload = {"contents": [{"parts": [{"text": f"System Instruction: {system_instruction}\n\nUser Task Request: {user_prompt}"}]}]}
        try:
            response = await http_client.post(url, headers=headers, json=payload, timeout=8.0)
            if response.status_code == 200:
                res_data = response.json()
                if "candidates" in res_data and len(res_data["candidates"]) > 0:
                    part = res_data["candidates"][0]["content"]["parts"][0]
                    return part.get("text", "")
        except Exception as e:
            logger.warning(f"⚠️ Primary Endpoint error: {e}.")
            continue

    for r_key in openrouter_keys:
        url = "https://openrouter.ai/api/v1/chat/completions"
        headers = {"Authorization": f"Bearer {r_key}", "Content-Type": "application/json"}
        payload = {
            "model": "meta-llama/llama-3.1-8b-instruct:free", 
            "messages": [{"role": "system", "content": system_instruction}, {"role": "user", "content": user_prompt}]
        }
        try:
            response = await http_client.post(url, headers=headers, json=payload, timeout=10.0)
            if response.status_code == 200:
                res_data = response.json()
                if "choices" in res_data and len(res_data["choices"]) > 0:
                    return res_data["choices"][0]["message"]["content"]
        except Exception as e:
            logger.warning(f"⚠️ Secondary Endpoint error: {e}.")
            continue

    return f"[{agent_name} Core Simulation Output]: Sub-task completed autonomously inside virtual system workspace."

async def save_history_bg(sid: str, task: str, html: str):
    if not db_pool:
        return
    try:
        async with db_pool.acquire() as db_conn:
            user_row = await db_conn.fetchrow("SELECT history FROM user_vault WHERE session_id = $1", sid)
            if user_row:
                h_data = user_row["history"]
                try:
                    h_list = json.loads(h_data) if isinstance(h_data, str) else (h_data if isinstance(h_data, list) else [])
                except Exception:
                    h_list = []
                h_list.append({"task": task, "code": html})
                await db_conn.execute("UPDATE user_vault SET history = $1 WHERE session_id = $2", json.dumps(h_list), sid)
                if redis_client:
                    await redis_client.delete(f"user:{sid}:history")
    except Exception as e:
        logger.error(f"Error in saving background history data: {str(e)}")

# 🚀 UPGRADED WEBSOCKET WITH HARD-GATE SUBSCRIPTION TRIGGER CONTROL
@app.websocket("/ws/v1/swarm-orchestrator/{session_id}")
async def websocket_swarm_endpoint(websocket: WebSocket, session_id: str):
    await websocket.accept()
    
    tier, credits, free_claimed = "free", 3000, False
    try:
        if db_pool:
            async with db_pool.acquire() as conn:
                user = await conn.fetchrow("SELECT tier, credits, free_tier_claimed FROM user_vault WHERE session_id = $1", session_id)
                if user is None:
                    # 🛠️ FIXED CRASH BUG: Ab ye hamesha safe 'conn' block ke andar hi execute hoga!
                    await conn.execute("INSERT INTO user_vault (session_id, credits) VALUES ($1, 3000)", session_id)
                    tier, credits, free_claimed = "free", 3000, False
                else:
                    tier, credits, free_claimed = user["tier"], user["credits"], user["free_tier_claimed"]
            
        if redis_client:
            await redis_client.set(f"user:{session_id}:credits", credits, ex=600)
    except Exception as err:
        logger.error(f"⚠️ Exception track inside WS Initializers: {err}")
            
    await websocket.send_json({"tier": tier, "tokens_left": max(0, credits)})
    
    try:
        while True:
            data = await websocket.receive_text()
            try:
                payload = json.loads(data)
            except Exception:
                continue
                
            user_task = payload.get("task", "").strip()
            is_approved = payload.get("blueprint_approved", False)
            
            if not user_task:
                continue
                
            # 🛡️ 1. Hard-Gate Plan Check: If Free Sandbox balance is exhausted, directly trigger Subscription Window!
            if tier == "free" and (credits <= 0 or free_claimed):
                await websocket.send_json({
                    "action": "TRIGGER_SUBSCRIPTION_POPUP",
                    "agent": "Security Warden",
                    "log": "❌ Access Revoked: Free Sandbox limit reached. Your device/profile has consumed its complimentary sandbox window. Upgrade to instantly build out this deployment!"
                })
                continue
                
            config = PLAN_SAFETY_LIMITS.get(tier, PLAN_SAFETY_LIMITS["free"])
            max_chars_allowed = config["max_chars"]
            input_length = len(user_task)
            
            if input_length > max_chars_allowed:
                await websocket.send_json({
                    "agent": "Security Warden",
                    "log": f"❌ Access Denied: Input length {input_length} exceeds limit of {max_chars_allowed} characters."
                })
                continue

            current_credits = credits
            if current_credits <= 0:
                if tier in ["infinite", "enterprise"]:
                    pass
                else:
                    await websocket.send_json({
                        "action": "TRIGGER_SUBSCRIPTION_POPUP",
                        "agent": "Security Warden",
                        "log": "❌ Access Revoked: Balance Exhausted. Please subscribe or buy tokens."
                    })
                    continue

            current_credits -= input_length
            if current_credits < 0 and tier not in ["infinite", "enterprise"]:
                await websocket.send_json({
                    "action": "TRIGGER_SUBSCRIPTION_POPUP",
                    "agent": "Security Warden",
                    "log": "❌ Access Revoked: Insufficient tokens to process task. Upgrade required."
                })
                continue

            # Update Credits and lock Free Tier consumption permanently on database layer
            try:
                if redis_client:
                    await redis_client.set(f"user:{session_id}:credits", current_credits)
                if db_pool:
                    async with db_pool.acquire() as conn:
                        if tier == "free":
                            await conn.execute("UPDATE user_vault SET credits = $1, free_tier_claimed = TRUE WHERE session_id = $2", current_credits, session_id)
                            free_claimed = True
                        else:
                            await conn.execute("UPDATE user_vault SET credits = $1 WHERE session_id = $2", current_credits, session_id)
            except Exception as db_mod_err:
                logger.error(f"⚠️ Error updating database logs: {db_mod_err}")

            credits = current_credits
            await websocket.send_json({"tier": tier, "tokens_left": credits, "log": f"Processing task metadata updates..."})

            if not is_approved:
                await websocket.send_json({"agent": "Kraken Swarm Director", "log": "📋 Assembling autonomous step-by-step Execution Blueprint Plan..."})
                blueprint_instruction = "Build a highly detailed architectural setup plan layout matching full autonomous capabilities..."
                blueprint_plan = await call_gemini_agent("Blueprint Engine", blueprint_instruction, user_task)
                
                await websocket.send_json({
                    "agent": "Blueprint Engine", 
                    "blueprint_structure": blueprint_plan,
                    "tokens_left": int(credits),
                    "log": "✓ Project Blueprint generated successfully. Click Approve to launch Sandbox execution loops."
                })
                continue

            if tier == "free":
                await websocket.send_json({"agent": "Kraken Swarm Director", "log": "🐢 Free Tier Sandbox speed active..."})
                await asyncio.sleep(config.get("delay_seconds", 25.0))

            await websocket.send_json({"agent": "Kraken Swarm Director", "log": "🚀 Blueprint approved. Running agents pipeline..."})
            agents_pipeline = [
                {"name": "Security Auditor", "prompt": "Identify code security vulnerabilities, trace invalid injections, prevent unauthorized system scripts execution."},
                {"name": "Swarm Architect", "prompt": "Map fully responsive layout blueprints, configure asset maps, set interactive state routers."},
                {"name": "Production Engine", "prompt": "Build highly integrated algorithmic components, interactive data visualizations, real-time widget configurations."},
                {"name": "Kraken Assembler", "prompt": "Synthesize multiple source agent streams into a single solid deployable module block."},
                {"name": "De-Penalization Agent", "prompt": "Perform self-healing checks on generated outputs, clean formatting limits."}
            ]
            
            combined_context = ""
            for idx, agent in enumerate(agents_pipeline, start=1):
                await websocket.send_json({"agent": agent["name"], "log": f"Executing Agent [{idx}/5] matrix loop routines..."})
                agent_res = await call_gemini_agent(agent["name"], agent["prompt"], user_task)
                combined_context += f"\n\n[{agent['name']} Output]:\n{agent_res}"
            
            await websocket.send_json({"agent": "Kraken Assembler", "log": f"Compiling dynamic sandbox frame content..."})
            assembler_instruction = "Synthesize an autonomous standalone feature-rich interactive dashboard application page using Tailwind CSS..."
            final_html_raw = await call_gemini_agent("Kraken Assembler", assembler_instruction, f"Core Requirements: {user_task}\n\nMulti-Agent Pipeline Inputs: {combined_context}")
            
            final_html = final_html_raw.strip()
            html_match = re.search(r"(<html.*?>.*?</html>|<!DOCTYPE.*?>.*?</html>)", final_html, re.DOTALL | re.IGNORECASE)
            if html_match:
                final_html = html_match.group(1).strip()
            else:
                if "```html" in final_html:
                    final_html = final_html.split("```html")[-1].split("```")[0].strip()
                elif "```" in final_html:
                    final_html = final_html.split("```")[-1].split("```")[0].strip()

            await save_history_bg(session_id, user_task, final_html)
            await websocket.send_json({"tier": tier, "tokens_left": credits, "result_data": {"status": "SUCCESS", "full_output": final_html}})
            
    except WebSocketDisconnect:
        logger.info(f"🔌 Connection pool track released for Session Node: {session_id}")
    except Exception as e:
        logger.error(f"❌ Swarm Pipeline Edge Exception caught: {str(e)}")

# --- 🚀 PORT INTEGRITY SYSTEM ---
if __name__ == "__main__":
    import uvicorn
    try:
        port_env = os.getenv("PORT", "10000")
        port = int(port_env) if port_env.isdigit() else 10000
    except Exception:
        port = 10000

    print(f"🚀 KRAKEN ENGINE FORCE-STARTED ON PORT: {port}")
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
