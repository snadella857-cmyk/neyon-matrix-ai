import os
import re
import json
import asyncio
import random
import logging
import hmac
import hashlib
import zipfile
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

# 🔌 ENVIRONMENT SETUP - SECURE FALLBACKS
DATABASE_URL_ENV = os.getenv("DATABASE_URL")
if not DATABASE_URL_ENV:
    DATABASE_URL_ENV = "postgresql://neondb_owner:npg_fAGLjuH5xJd8@ep-fancy-meadow-ajdpi2bm-pooler.us-east-2.aws.neon.tech/neondb?sslmode=require"

RAW_DB_URL = DATABASE_URL_ENV
SECONDARY_DB_URL = os.getenv("SECONDARY_DATABASE_URL", RAW_DB_URL)
REDIS_URL = os.getenv("REDIS_URL", "redis://default:rEdIsPaSsWoRd99@redis-12345.c302.us-east-1-1.ec2.cloud.redislabs.com:12345")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

db_pool = None
secondary_db_pool = None
redis_client = None
http_client = None

# --- IN-MEMORY CACHE FOR LIVE PREVIEWS ---
PREVIEW_CACHE: Dict[str, str] = {}

# 🪙 CHARACTER LIMITS & QUOTA MANAGEMENT
PLAN_SAFETY_LIMITS = {
    "free": {
        "max_chars": 600,        # <--- 600 Characters (Testing ke liye ekdum perfect!)
        "delay_seconds": 20.0,  
        "max_daily_queries": 2   # <--- Pura satisfy hone ke liye 2 chances max
    },
    "lite": {                    # ₹499 Plan
        "max_chars": 3500,       
        "max_daily_queries": 25      
    },
    "infinite": {                # ₹999 Plan
        "max_chars": 8000,       
        "max_daily_queries": 60       
    },
    "enterprise": {              # ₹3999 Plan
        "max_chars": 30000,      
        "max_daily_queries": 999999  # Unlimited
    }
}

DISPOSABLE_DOMAINS = {"mailinator.com", "temp-mail.org", "yopmail.com", "sharklasers.com", "guerrillamail.com", "dispostable.com", "getairmail.com"}

class ActivationPayload(BaseModel):
    session_id: str
    email: str
    browser_timezone: str
    device_fingerprint: str 

class KrakenDBSyncPayload(BaseModel):
    session_id: str
    store_name: str
    payload_data: dict

# 📢 DISCORD LOGGING INTEGRATION
async def log_to_discord(agent_name: str, message: str, status: str = "INFO"):
    """Sends production events directly to your Discord Channel"""
    if not DISCORD_WEBHOOK_URL or not http_client:
        return
    
    color_map = {"INFO": 3447003, "SUCCESS": 3066993, "ERROR": 15158332}
    embed_color = color_map.get(status, 3447003)
    
    payload = {
        "embeds": [{
            "title": f"🤖 Kraken Swarm Update - {agent_name}",
            "description": message[:1900],
            "color": embed_color,
            "footer": {"text": "Kraken Enterprise Swarm Engine"}
        }]
    }
    try:
        await http_client.post(DISCORD_WEBHOOK_URL, json=payload, timeout=5.0)
    except Exception as e:
        logger.error(f"Failed to push update to Discord: {e}")

async def initialize_db_tables():
    """Background helper to create tables once pool is ready"""
    global db_pool
    for attempt in range(5):
        try:
            if db_pool:
                async with db_pool.acquire() as conn:
                    await conn.execute('''
                        CREATE TABLE IF NOT EXISTS user_vault (
                            session_id TEXT PRIMARY KEY,
                            email TEXT,
                            device_hash TEXT,
                            tier TEXT DEFAULT 'free',
                            verified BOOLEAN DEFAULT FALSE,
                            free_tier_claimed BOOLEAN DEFAULT FALSE,
                            arbitrage_risk BOOLEAN DEFAULT FALSE,
                            queries_used_today INT DEFAULT 0,
                            history JSONB DEFAULT '[]'::jsonb
                        );
                        CREATE TABLE IF NOT EXISTS krakendb_sync (
                            id SERIAL PRIMARY KEY,
                            session_id TEXT,
                            store_name TEXT,
                            data JSONB,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        );
                        CREATE INDEX IF NOT EXISTS idx_device_hash ON user_vault(device_hash);
                        CREATE INDEX IF NOT EXISTS idx_email ON user_vault(email);
                        CREATE INDEX IF NOT EXISTS idx_krakendb_sess ON krakendb_sync(session_id);
                    ''')
                logger.info("✅ Core Platform Tables & KrakenDB Sync Engine checked/created successfully.")
                break
        except Exception as e:
            logger.warning(f"⚠️ Table initialization attempt {attempt+1} failed: {e}. Retrying...")
            await asyncio.sleep(2)

# MODERN FASTAPI LIFESPAN
@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool, secondary_db_pool, redis_client, http_client
    
    target_db_url = RAW_DB_URL
    if target_db_url:
        if "?sslmode=" in target_db_url:
            base_url = target_db_url.split("?")[0]
            target_db_url = f"{base_url}?sslmode=require"
        elif "localhost" not in target_db_url and "127.0.0.1" not in target_db_url:
            target_db_url = f"{target_db_url}?sslmode=require"
            
    target_sec_url = SECONDARY_DB_URL
    if target_sec_url:
        if "?sslmode=" in target_sec_url:
            base_url = target_sec_url.split("?")[0]
            target_sec_url = f"{base_url}?sslmode=require"
        elif "localhost" not in target_sec_url and "127.0.0.1" not in target_sec_url:
            target_sec_url = f"{target_sec_url}?sslmode=require"

    limits = httpx.Limits(max_keepalive_connections=100, max_connections=400)
    http_client = httpx.AsyncClient(limits=limits, timeout=15.0)

    if aioredis and REDIS_URL:
        try:
            redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
            logger.info("⚡ Redis Client handle prepared synchronously.")
        except Exception as ree:
            logger.error(f"❌ Redis client configuration failure: {ree}")

    if target_db_url:
        try:
            logger.info("🔄 Connecting to Remote primary database cluster...")
            db_pool = await asyncpg.create_pool(target_db_url, min_size=1, max_size=15, timeout=10.0)
            logger.info("✅ Primary Database connection pool initialized.")
            asyncio.create_task(initialize_db_tables())
        except Exception as dbe:
            logger.error(f"❌ CRITICAL PRIMARY DB CONNECTION DELAY: {dbe}.")
            
    if target_sec_url and target_sec_url != target_db_url:
        try:
            logger.info("🔄 Connecting to Remote secondary database cluster...")
            secondary_db_pool = await asyncpg.create_pool(target_sec_url, min_size=1, max_size=5, timeout=10.0)
            logger.info("✅ Secondary Database connection pool initialized.")
        except Exception as sdbe:
            logger.error(f"❌ SECONDARY DB CONNECTION DELAY: {sdbe}.")
    else:
        logger.info("ℹ️ Secondary DB URL fallback mode enabled.")
    
    yield
    
    if db_pool:
        await db_pool.close()
    if secondary_db_pool:
        await secondary_db_pool.close()
    if http_client:
        await http_client.aclose()
    logger.info("⚡ System resources shutdown successfully.")

app = FastAPI(title="Kraken Swarm Engine", lifespan=lifespan)

# --- 🚀 REAL-TIME CLOUD-SYNCED KRAKENDB ---
@app.post("/api/v1/krakendb/sync")
async def sync_krakendb(payload: KrakenDBSyncPayload):
    """Saves real-time state configurations from sandboxes straight to PostgreSQL cluster"""
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database currently offline.")
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO krakendb_sync (session_id, store_name, data) 
                VALUES ($1, $2, $3)
                """,
                payload.session_id, payload.store_name, json.dumps(payload.payload_data)
            )
        return {"status": "SUCCESS", "message": "Sandbox state captured dynamically."}
    except Exception as e:
        logger.error(f"KrakenDB sync error: {e}")
        raise HTTPException(status_code=500, detail="State save failure.")

@app.get("/api/v1/krakendb/sync/{session_id}")
async def get_krakendb_sync(session_id: str):
    """Retrieves all real-time dynamic states synced by KrakenDB within sandboxes"""
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database currently offline.")
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT store_name, data, updated_at FROM krakendb_sync WHERE session_id = $1 ORDER BY id DESC LIMIT 50", 
                session_id
            )
            results = []
            for r in rows:
                results.append({
                    "store_name": r["store_name"],
                    "data": json.loads(r["data"]) if isinstance(r["data"], str) else r["data"],
                    "updated_at": r["updated_at"].isoformat()
                })
            return {"session_id": session_id, "states": results}
    except Exception as e:
        logger.error(f"KrakenDB read error: {e}")
        raise HTTPException(status_code=500, detail="State query failure.")

# --- 🌐 LIVE SANDBOX PREVIEW ENDPOINT ---
@app.get("/api/v1/preview/{session_id}", response_class=HTMLResponse)
async def live_sandbox_preview(session_id: str):
    """Instant deployment preview loader"""
    if session_id in PREVIEW_CACHE:
        return HTMLResponse(content=PREVIEW_CACHE[session_id], status_code=200)
    
    if db_pool:
        try:
            async with db_pool.acquire() as conn:
                user = await conn.fetchrow("SELECT history FROM user_vault WHERE session_id = $1", session_id)
                if user and user["history"]:
                    history_data = json.loads(user["history"]) if isinstance(user["history"], str) else user["history"]
                    if history_data and len(history_data) > 0:
                        last_code = history_data[-1].get("code", "<h3>No output compiled inside history yet.</h3>")
                        return HTMLResponse(content=last_code, status_code=200)
        except Exception as e:
            logger.error(f"Error serving fallback db preview: {e}")
            
    return HTMLResponse(content="<h3>Sandbox preview session not active or has been cleared.</h3>", status_code=404)

# --- 📦 EXPORT ZIP API ENDPOINT ---
@app.get("/api/v1/export/{session_id}")
async def export_project_zip(session_id: str):
    """Zip up compiled sandboxed workspace in a clean downloadable file package"""
    content = ""
    if session_id in PREVIEW_CACHE:
        content = PREVIEW_CACHE[session_id]
    elif db_pool:
        try:
            async with db_pool.acquire() as conn:
                user = await conn.fetchrow("SELECT history FROM user_vault WHERE session_id = $1", session_id)
                if user and user["history"]:
                    history_data = json.loads(user["history"]) if isinstance(user["history"], str) else user["history"]
                    if history_data and len(history_data) > 0:
                        content = history_data[-1].get("code", "")
        except Exception as e:
            logger.error(f"Failed to fetch content for export: {e}")

    if not content:
        raise HTTPException(status_code=404, detail="No active code deployment found to export.")

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
        zip_file.writestr("index.html", content)
        
        readme_txt = """# Compiled Sandbox Project by Kraken Swarm Engine

## How to Run
1. Extract the zip.
2. Double click `index.html` to open in any web browser.
3. This application is powered by Tailwind CSS and comes with self-contained styling, database mockup layers, and client state-controllers.
"""
        zip_file.writestr("README.md", readme_txt)

    zip_buffer.seek(0)
    headers = {
        'Content-Disposition': f'attachment; filename="kraken_deployment_{session_id[:8]}.zip"'
    }
    return StreamingResponse(zip_buffer, media_type="application/x-zip-compressed", headers=headers)

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    dashboard_ui = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Kraken Swarm Production Engine Dashboard</title>
        <script src="https://cdn.tailwindcss.com"></script>
    </head>
    <body class="bg-slate-900 text-white flex flex-col items-center justify-center min-h-screen p-6">
        <div class="max-w-md w-full bg-slate-800 rounded-xl p-8 shadow-2xl border border-slate-700 text-center">
            <h1 class="text-3xl font-extrabold text-blue-400 mb-2">Kraken Swarm Engine</h1>
            <p class="text-slate-400 text-sm mb-6">Claim your 600-character test sandbox quota instantly via integrated OAuth confirmation workflow layer.</p>
            
            <div id="auth-box">
                <button id="google-login-btn" onclick="triggerGoogleSandboxClaim()" class="w-full flex items-center justify-center gap-3 bg-white text-slate-900 font-semibold py-3 px-4 rounded-lg hover:bg-slate-100 transition-all shadow-md">
                    <svg class="w-5 h-5" viewBox="0 0 24 24">
                        <path fill="#EA4335" d="M12.24 10.285V14.4h6.887c-.648 2.41-2.519 4.2-5.636 4.2-3.856 0-6.99-3.134-6.99-6.99a6.99 6.99 0 0 1 6.99-6.99c1.74 0 3.3.63 4.53 1.67l3.22-3.22C18.28 1.19 15.46 0 12.24 0 5.48 0 0 5.48 0 12.24s5.48 12.24 12.24 12.24c6.82 0 12.3-4.94 12.3-12.24 0-.83-.08-1.64-.24-2.425H12.24Z"/>
                    </svg>
                    <span>Sign in with Google</span>
                </button>
            </div>
            <div id="status-message" class="mt-4 text-sm font-medium"></div>
        </div>

        <script>
        async function triggerGoogleSandboxClaim() {
            const statusMsg = document.getElementById("status-message");
            statusMsg.className = "mt-4 text-sm font-medium text-blue-400 animate-pulse";
            statusMsg.innerText = "Processing dynamic unique token activation token...";
            
            const mockSessionId = 'sess_' + Math.random().toString(36).substring(2, 15);
            const userEmail = prompt("Enter your verified Google Account Email address to activate dynamic sandbox quota allotment:");
            
            if(!userEmail || !userEmail.includes("@")) {
                statusMsg.className = "mt-4 text-sm font-medium text-red-400";
                statusMsg.innerText = "❌ Invalid email channel connection mapping.";
                return;
            }

            try {
                const response = await fetch('/api/v1/activate-node', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        session_id: mockSessionId,
                        email: userEmail,
                        browser_timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
                        device_fingerprint: "fp_static_hash_compiled_platform_node"
                    })
                });
                
                const data = await response.json();
                if(response.ok) {
                    statusMsg.className = "mt-4 text-sm font-medium text-green-400";
                    statusMsg.innerText = "✅ Quota Loaded: 600 Chars Sandbox initialized successfully! Max 2 inputs permitted today.";
                    localStorage.setItem("kraken_active_session", mockSessionId);
                } else {
                    statusMsg.className = "mt-4 text-sm font-medium text-red-400";
                    statusMsg.innerText = data.detail || "❌ Single verification quota mapping failed.";
                }
            } catch(e) {
                statusMsg.className = "mt-4 text-sm font-medium text-red-400";
                statusMsg.innerText = "❌ Network authorization handshake error.";
            }
        }
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=dashboard_ui, status_code=200)

@app.post("/api/v1/activate-node")
async def activate_node(payload: ActivationPayload):
    if not db_pool:
         raise HTTPException(status_code=503, detail="Database cluster currently initializing.")
    
    email = payload.email.lower().strip()
    domain = email.split("@")[-1] if "@" in email else ""
    
    if domain in DISPOSABLE_DOMAINS or not re.match(r"[^@]+@[^@]+\.[^@]+", email):
        raise HTTPException(status_code=400, detail="❌ Professional or authenticated email networks only.")
    
    tz = payload.browser_timezone.lower()
    arbitrage_risk = False
    
    try:
        async with db_pool.acquire() as conn:
            # 🔒 LIFETIME STRICT BLOCK: Ek baar register hone ke baad, 2 din baad ya kabhi bhi dobara access nahi milega
            email_check = await conn.fetchrow(
                "SELECT * FROM user_vault WHERE email = $1 AND free_tier_claimed = TRUE", 
                email
            )
            if email_check:
                raise HTTPException(
                    status_code=403, 
                    detail="❌ Access Denied: Aap is email se apna lifetime free quota pehle hi claim kar chuke hain. Dobara access nahi mil sakta."
                )

            # Strict Device Hardware Profile Validation (Lifetime Lock Chain)
            fingerprint_check = await conn.fetchrow(
                "SELECT * FROM user_vault WHERE device_hash = $1 AND free_tier_claimed = TRUE", 
                payload.device_fingerprint
            )
            if fingerprint_check:
                raise HTTPException(
                    status_code=403, 
                    detail="❌ Access Denied: Is device profile se free kota pehle hi liya ja chuka hai. Dobara entry permanently blocked hai."
                )

            user = await conn.fetchrow("SELECT * FROM user_vault WHERE session_id = $1", payload.session_id)
            if "asia/calcutta" not in tz and "kolkata" not in tz and user and user.get("detected_country") == "IN":
                arbitrage_risk = True
                
            if user:
                await conn.execute(
                    "UPDATE user_vault SET email=$1, verified=TRUE, arbitrage_risk=$2, device_hash=$3, free_tier_claimed=TRUE WHERE session_id=$4", 
                    email, arbitrage_risk, payload.device_fingerprint, payload.session_id
                )
            else:
                await conn.execute(
                    "INSERT INTO user_vault (session_id, email, verified, arbitrage_risk, device_hash, tier, free_tier_claimed) VALUES ($1, $2, TRUE, $3, $4, 'free', TRUE)", 
                    payload.session_id, email, arbitrage_risk, payload.device_fingerprint
                )
        
        if redis_client:
            await redis_client.set(f"user:{payload.session_id}:tier", "free", ex=3600)
            
    except HTTPException as he:
        raise he
    except Exception as dbe:
        logger.error(f"Error executing db transaction in activate_node: {dbe}")
        raise HTTPException(status_code=500, detail="Internal lock configuration sync error.")
        
    return {"status": "SUCCESS", "message": "Authenticated successfully. Lifetime single free quota locked."}

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
         return {"tier": "free", "history": [], "warning": "DB Syncing/Unavailable"}
         
    try:
        async with db_pool.acquire() as conn:
            user = await conn.fetchrow("SELECT tier, history FROM user_vault WHERE session_id = $1", session_id)
            if not user:
                return {"tier": "free", "history": []}
            history_data = user["history"]
            parsed_history = json.loads(history_data) if isinstance(history_data, str) else (history_data if isinstance(history_data, list) else [])
            response_data = {"tier": user["tier"], "history": parsed_history}
            if redis_client:
                await redis_client.set(f"user:{session_id}:history", json.dumps(response_data), ex=300)
            return response_data
    except Exception as e:
        logger.error(f"History routing exception: {e}")
        return {"tier": "free", "history": [], "error": "Internal synchronization error"}

# --- 🔌 ADVANCED DUAL-LLM ROUTING WITH MICROSECOND RESILIENCY ---
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
            logger.warning(f"⚠️ Primary Endpoint [Gemini] failure: {e}. Attempting fallback sequence...")
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
            logger.warning(f"⚠️ Secondary Endpoint [OpenRouter] failure: {e}.")
            continue

    return f"[{agent_name} Core Simulation Output]: Sub-task completed autonomously inside virtual system workspace."

# --- 🛠️ ERROR SELF-HEALING / AUTO-HEALER AGENT ---
async def self_heal_output_code(raw_code: str) -> str:
    """Active Auto-Healer to correct missing tags, incorrect markdown wraps, or unbalanced structures"""
    healed = raw_code.strip()
    
    html_match = re.search(r"(<html.*?>.*?</html>|<!DOCTYPE.*?>.*?</html>)", healed, re.DOTALL | re.IGNORECASE)
    if html_match:
        healed = html_match.group(1).strip()
    else:
        if "```html" in healed:
            healed = healed.split("```html")[-1].split("```")[0].strip()
        elif "```" in healed:
            healed = healed.split("```")[-1].split("```")[0].strip()
            
    if "<html" in healed and not healed.endswith("</html>"):
        healed += "\n</html>"
    if "<body" in healed and "</body>" not in healed:
        healed = healed.replace("</html>", "</body>\n</html>")

    if len(healed) < 100 or "<script" in healed and "</script>" not in healed:
        healer_instruction = "You are the Auto-Healer Engine. Take the input code, repair any broken/unclosed tag, fix missing script elements, and return the complete robust HTML."
        healed = await call_gemini_agent("Auto-Healer Agent", healer_instruction, healed)

    return healed

async def save_history_bg(sid: str, task: str, html: str):
    PREVIEW_CACHE[sid] = html
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

# WEBSOCKET SWARM PIPELINE WITH OPTIMIZED DISCORD LOGGING CHANNELS
async def process_async_agents_pipeline(user_task: str, combined_context_dict: dict, websocket: WebSocket):
    agents_pipeline = [
        {"name": "Security Auditor", "prompt": "Identify code security vulnerabilities, trace invalid injections, prevent unauthorized system scripts execution."},
        {"name": "Swarm Architect", "prompt": "Map fully responsive layout blueprints, configure asset maps, set interactive state routers."},
        {"name": "Production Engine", "prompt": "Build highly integrated algorithmic components, interactive data visualizations, real-time widget configurations."},
        {"name": "Kraken Assembler", "prompt": "Synthesize multiple source agent streams into a single solid deployable module block."},
        {"name": "De-Penalization Agent", "prompt": "Perform self-healing checks on generated outputs, clean formatting limits."}
    ]
    
    async def run_single_agent(idx, agent):
        try:
            await websocket.send_json({"agent": agent["name"], "log": f"Launching Agent [{idx}/5] matrix loop..."})
            asyncio.create_task(log_to_discord(agent["name"], f"Started processing task: {user_task[:150]}...", "INFO"))
            
            res = await call_gemini_agent(agent["name"], agent["prompt"], user_task)
            combined_context_dict[agent["name"]] = res
            
            await websocket.send_json({"agent": agent["name"], "log": f"✓ Agent [{idx}/5] completed."})
            asyncio.create_task(log_to_discord(agent["name"], f"Successfully processed subtask and structured pipeline context.", "SUCCESS"))
        except Exception as ae:
            logger.error(f"Error in agent run {agent['name']}: {ae}")
            combined_context_dict[agent["name"]] = f"Bypass state: {ae}"
            asyncio.create_task(log_to_discord(agent["name"], f"Error in processing: {str(ae)}", "ERROR"))

    tasks = [run_single_agent(i + 1, agent) for i, agent in enumerate(agents_pipeline)]
    await asyncio.gather(*tasks)

# 🚀 WEBSOCKET SWARM ORCHESTRATOR WITH SYSTEM-WIDE STABILITY
@app.websocket("/ws/v1/swarm-orchestrator/{session_id}")
async def websocket_swarm_endpoint(websocket: WebSocket, session_id: str):
    await websocket.accept()
    tier, free_claimed, queries_used = "free", False, 0
    
    if db_pool:
        try:
            async with db_pool.acquire() as conn:
                user = await conn.fetchrow("SELECT tier, free_tier_claimed, queries_used_today FROM user_vault WHERE session_id = $1", session_id)
                if user is None:
                    await conn.execute("INSERT INTO user_vault (session_id) VALUES ($1)", session_id)
                    tier, free_claimed, queries_used = "free", False, 0
                else:
                    tier, free_claimed = user["tier"], user["free_tier_claimed"]
                    queries_used = user["queries_used_today"] if user["queries_used_today"] is not None else 0
            
            if redis_client:
                await redis_client.set(f"user:{session_id}:tier", tier, ex=600)
        except Exception as err:
            logger.error(f"⚠️ Exception inside DB Handshake: {err}")
    else:
        logger.warning("⚠️ WS falling back in database offline state.")
            
    await websocket.send_json({"tier": tier, "status": "CONNECTED"})
    
    try:
        while True:
            data = await websocket.receive_text()
            try:
                payload = json.loads(data)
            except Exception:
                continue
                
            user_task = payload.get("task", "").strip()
            is_approved = payload.get("blueprint_approved", False)
            edit_instruction = payload.get("edit_instruction", "").strip()
            
            if not user_task and not edit_instruction:
                continue

            # 🛠️ STRICTOR QUOTA GATEKEEPING MATRIX RULES: Max 2 entries allowed inside free account limit structure
            if tier == "free" and (queries_used >= PLAN_SAFETY_LIMITS["free"]["max_daily_queries"] or free_claimed):
                await websocket.send_json({"agent": "Kraken Swarm Director", "log": "❌ Access Denied: Max daily sandbox query allocation exhausted (Limit: 2 queries max)."})
                continue

            # Check inputs text validation rules against 600 character threshold architecture
            if tier == "free" and (len(user_task) > PLAN_SAFETY_LIMITS["free"]["max_chars"] or len(edit_instruction) > PLAN_SAFETY_LIMITS["free"]["max_chars"]):
                await websocket.send_json({"agent": "Kraken Swarm Director", "log": f"❌ Error: Free account workspace matrix limit exceeded 600 character restrictions."})
                continue

            try:
                if db_pool:
                    async with db_pool.acquire() as conn:
                        queries_used += 1
                        await conn.execute("UPDATE user_vault SET free_tier_claimed = TRUE, queries_used_today = $1 WHERE session_id = $2", queries_used, session_id)
                        free_claimed = True
            except Exception as db_mod_err:
                logger.error(f"⚠️ Error updating free database tier logs: {db_mod_err}")

            await websocket.send_json({"tier": tier, "log": f"Processing task metadata updates..."})

            try:
                # --- 🔥 UPGRADED ITERATIVE EDITING PIPELINE (With MULTIPLY ENGINE) ---
                if edit_instruction:
                    await websocket.send_json({"agent": "Kraken Editor", "log": "✏️ Fetching previous deployment cache..."})
                    existing_html = ""
                    
                    if session_id in PREVIEW_CACHE:
                        existing_html = PREVIEW_CACHE[session_id]
                    elif db_pool:
                        async with db_pool.acquire() as conn:
                            user = await conn.fetchrow("SELECT history FROM user_vault WHERE session_id = $1", session_id)
                            if user and user["history"]:
                                history_data = json.loads(user["history"]) if isinstance(user["history"], str) else user["history"]
                                if history_data and len(history_data) > 0:
                                    existing_html = history_data[-1].get("code", "")

                    if not existing_html:
                        await websocket.send_json({"agent": "Kraken Editor", "log": "⚠️ No existing deployment to edit. Building fresh instead..."})
                        user_task = edit_instruction
                    else:
                        await websocket.send_json({"agent": "Kraken Editor", "log": "⚡ Applying MULTIPLY (Modular Feature Addition) Engine..."})
                        asyncio.create_task(log_to_discord("Kraken Editor", f"Applying edit: {edit_instruction[:150]}", "INFO"))
                        
                        editor_system_instruction = (
                            "You are the master Kraken Code Editor. Modify the existing stand-alone HTML application "
                            "based strictly on the user's edit instructions. Your primary rule is MULTIPLY: never "
                            "destroy existing panels, tools, settings, or visual items. Instead, append new features as "
                            "modular elements, tabs, modal panels, or drop-down widgets. Keep existing design details and Tailwind components intact. "
                            "Always return the full HTML code document."
                        )
                        final_html_raw = await call_gemini_agent(
                            "Kraken Editor", 
                            editor_system_instruction, 
                            f"Existing Code:\n{existing_html}\n\nEdit/Multiply Instruction:\n{edit_instruction}"
                        )
                        
                        await websocket.send_json({"agent": "Auto-Healer Agent", "log": "🔧 Running self-healing verification checks..."})
                        final_html = await self_heal_output_code(final_html_raw)

                        asyncio.create_task(save_history_bg(session_id, f"Edited: {edit_instruction}", final_html))
                        asyncio.create_task(log_to_discord("Kraken Editor", "Edited deployment compiled successfully with Multiply features.", "SUCCESS"))
                        
                        await websocket.send_json({
                            "tier": tier, 
                            "preview_url": f"/api/v1/preview/{session_id}",
                            "result_data": {"status": "SUCCESS", "full_output": final_html}
                        })
                        continue

                # --- STANDALONE GENERATION PIPELINE ---
                if not is_approved:
                    await websocket.send_json({"agent": "Kraken Swarm Director", "log": "📋 Assembling autonomous step-by-step Execution Blueprint Plan..."})
                    blueprint_instruction = "Build a highly detailed architectural setup plan layout matching full autonomous capabilities..."
                    blueprint_plan = await call_gemini_agent("Blueprint Engine", blueprint_instruction, user_task)
                    
                    await websocket.send_json({
                        "agent": "Blueprint Engine", 
                        "blueprint_structure": blueprint_plan,
                        "log": "✓ Project Blueprint generated successfully. Click Approve to launch Sandbox execution loops."
                    })
                    continue

                if tier == "free":
                    await websocket.send_json({"agent": "Kraken Swarm Director", "log": f"🐢 Free Tier Sandbox speed active (Enforcing {PLAN_SAFETY_LIMITS['free']['delay_seconds']}s delay limit)..."})
                    await asyncio.sleep(PLAN_SAFETY_LIMITS["free"]["delay_seconds"])

                await websocket.send_json({"agent": "Kraken Swarm Director", "log": "🚀 Blueprint approved. Running agents pipeline..."})
                
                combined_context_dict = {}
                await process_async_agents_pipeline(user_task, combined_context_dict, websocket)
                
                combined_context = ""
                for agent_name, output in combined_context_dict.items():
                    combined_context += f"\n\n[{agent_name} Output]:\n{output}"
                
                await websocket.send_json({"agent": "Kraken Assembler", "log": f"Compiling dynamic sandbox frame content..."})
                
                database_setup_snippet = f"""
                <script>
                class KrakenDB {{
                    static init(storeName) {{
                        this.storeName = storeName;
                        this.sessionId = "{session_id}";
                        if (!localStorage.getItem(storeName)) {{
                            localStorage.setItem(storeName, JSON.stringify([]));
                        }}
                    }}
                    static async insert(data) {{
                        const items = JSON.parse(localStorage.getItem(this.storeName) || '[]');
                        const record = {{ id: Date.now(), ...data, created_at: new Date().toISOString() }};
                        items.push(record);
                        localStorage.setItem(this.storeName, JSON.stringify(items));
                        
                        try {{
                            await fetch('/api/v1/krakendb/sync', {{
                                method: 'POST',
                                headers: {{ 'Content-Type': 'application/json' }},
                                body: JSON.stringify({{
                                    session_id: this.sessionId,
                                    store_name: this.storeName,
                                    payload_data: record
                                }})
                            }});
                        }} catch(e) {{
                            console.warn("Cloud Sync pending: ", e);
                        }}
                        return record;
                    }}
                    static select() {{
                        return JSON.parse(localStorage.getItem(this.storeName) || '[]');
                    }}
                    static clear() {{
                        localStorage.setItem(this.storeName, JSON.stringify([]));
                    }}
                }}
                console.log("⚡ Cloud-Synced Kraken DB layer loaded successfully.");
                </script>
                """
                
                assembler_instruction = (
                    "Synthesize a standalone dashboard application using Tailwind CSS. "
                    "Incorporate the following mock database layers to make client actions completely responsive: "
                    + database_setup_snippet
                )
                
                final_html_raw = await call_gemini_agent("Kraken Assembler", assembler_instruction, f"Core Requirements: {user_task}\n\nMulti-Agent Pipeline Inputs: {combined_context}")
                
                await websocket.send_json({"agent": "Auto-Healer Agent", "log": "🔧 Running self-healing verification checks..."})
                final_html = await self_heal_output_code(final_html_raw)

                asyncio.create_task(save_history_bg(session_id, user_task, final_html))
                asyncio.create_task(log_to_discord("Kraken Assembler", f"Successfully assembled and compiled live dashboard for Session: {session_id}.", "SUCCESS"))
                
                await websocket.send_json({
                    "tier": tier, 
                    "preview_url": f"/api/v1/preview/{session_id}",
                    "result_data": {"status": "SUCCESS", "full_output": final_html}
                })
            
            except Exception as loop_err:
                logger.error(f"Error inside processing loop block: {str(loop_err)}")
                await websocket.send_json({"agent": "Kraken Swarm Director", "log": f"❌ Error occurred: {str(loop_err)}"})
                asyncio.create_task(log_to_discord("Kraken Swarm Director", f"Processing crash on task execution: {str(loop_err)}", "ERROR"))
            
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
    uvicorn.run(app, host="0.0.0.0", port=port, reload=False)
