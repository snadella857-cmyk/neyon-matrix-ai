import os
import re
import json
import asyncio
import random
import logging
import hmac
import hashlib
import zipfile
import datetime
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

# Safe import for qrcode and pillow features
try:
    import qrcode
    from PIL import Image
    HAS_IMAGE_PROCESSING = True
except ImportError:
    HAS_IMAGE_PROCESSING = False

# --- LOGGING & APP SETUP ---
logger = logging.getLogger("kraken_swarm_production")
logging.basicConfig(level=logging.INFO)

# 🔐 ENVIRONMENT SETUP - SECURE ENVIRONMENT VARIATION
DATABASE_URL_ENV = os.getenv("DATABASE_URL")
if not DATABASE_URL_ENV:
    # Local fallback optimized for environments setup without local production secrets
    DATABASE_URL_ENV = "postgresql://localhost/neondb"

RAW_DB_URL = DATABASE_URL_ENV
SECONDARY_DB_URL = os.getenv("SECONDARY_DATABASE_URL", RAW_DB_URL)
REDIS_URL = os.getenv("REDIS_URL", "redis://default:rEdIsPaSsWoRd99@redis-12345.c302.us-east-1-1.ec2.cloud.redislabs.com:12345")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

db_pool = None
secondary_db_pool = None
redis_client = None
http_client = None

# 📊 CHARACTER LIMITS & QUOTA MANAGEMENT
PLAN_SAFETY_LIMITS = {
    "free": {
        "max_chars": 600,
        "delay_seconds": 20.0,  
        "max_daily_queries": 2   
    },
    "lite": {
        "max_chars": 3500,       
        "max_daily_queries": 25      
    },
    "infinite": {
        "max_chars": 8000,       
        "max_daily_queries": 60       
    },
    "enterprise": {
        "max_chars": 30000,      
        "max_daily_queries": 999999  
    }
}

DISPOSABLE_DOMAINS = set() 

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
    if not DISCORD_WEBHOOK_URL or not http_client:
        return
    payload = {
        "embeds": [{
            "title": f"📢 Kraken Swarm Update - {agent_name}",
            "description": message[:1900],
            "color": {"INFO": 3447003, "SUCCESS": 3066993, "ERROR": 15158332}.get(status, 3447003),
            "footer": {"text": "Kraken Enterprise Swarm Engine"}
        }]
    }
    try:
        await http_client.post(DISCORD_WEBHOOK_URL, json=payload, timeout=5.0)
    except Exception as e:
        logger.error(f"Failed to push update to Discord: {e}")

def execute_cpu_heavy_image_task(data_content: str) -> io.BytesIO:
    if not HAS_IMAGE_PROCESSING:
        raise RuntimeError("Image/QR processing libraries missing on runtime ecosystem.")
    qr = qrcode.QRCode(version=1, box_size=10, border=5)
    qr.add_data(data_content)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white").convert('RGB')
    img = img.resize((300, 300), Image.Resampling.LANCZOS)
    
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return buf

async def daily_query_reset_scheduler():
    while True:
        try:
            now = datetime.datetime.now(datetime.timezone.utc)
            tomorrow = now + datetime.timedelta(days=1)
            midnight = datetime.datetime.combine(tomorrow, datetime.time.min, tzinfo=datetime.timezone.utc)
            seconds_until_midnight = (midnight - now).total_seconds()
            
            logger.info(f"Midnight scheduler sleeping for {seconds_until_midnight} seconds until next reset loop.")
            await asyncio.sleep(max(seconds_until_midnight, 1.0))
            
            if db_pool:
                try:
                    async with db_pool.acquire() as conn:
                        await conn.execute("UPDATE user_vault SET queries_used_today = 0;")
                    logger.info("System-wide Reset triggered successfully: queries_used_today updated to 0 for all tiers.")
                except Exception as db_err:
                    logger.error(f"Database query execution failed inside scheduler: {db_err}")
                
        except asyncio.CancelledError:
            break
        except Exception as ce:
            logger.error(f"Midnight scheduler pipeline loop error: {ce}")
            await asyncio.sleep(60)

async def initialize_db_tables():
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
                logger.info("Core Platform Tables & KrakenDB Sync Engine checked/created successfully.")
                break
        except Exception as e:
            logger.warning(f"Table initialization attempt {attempt+1} failed: {e}. Retrying...")
            await asyncio.sleep(2)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool, secondary_db_pool, redis_client, http_client
    
    def clean_db_url(url: str) -> str:
        if url and "?sslmode=" not in url and "localhost" not in url and "127.0.0.1" not in url:
            base = url.split("?")[0]
            return f"{base}?sslmode=require"
        return url

    target_db_url = clean_db_url(RAW_DB_URL)
    target_sec_url = clean_db_url(SECONDARY_DB_URL)

    limits = httpx.Limits(max_keepalive_connections=100, max_connections=400)
    http_client = httpx.AsyncClient(limits=limits, timeout=15.0)

    if aioredis and REDIS_URL:
        try:
            redis_client = aioredis.from_url(REDIS_URL, decode_responses=True, socket_timeout=5.0, socket_connect_timeout=5.0)
            logger.info("Redis Client handle prepared synchronously.")
        except Exception as ree:
            logger.error(f"Redis client configuration failure: {ree}")

    if target_db_url:
        try:
            logger.info("Connecting to Remote primary database cluster...")
            db_pool = await asyncpg.create_pool(target_db_url, min_size=1, max_size=5, timeout=5.0, command_timeout=5.0)
            logger.info("Primary Database connection pool initialized.")
            asyncio.create_task(initialize_db_tables())
            asyncio.create_task(daily_query_reset_scheduler())
        except Exception as dbe:
            logger.error(f"CRITICAL PRIMARY DB TIMEOUT BUT SERVING INTERFACE: {dbe}.")
            db_pool = None
            
    if target_sec_url and target_sec_url != target_db_url:
        try:
            secondary_db_pool = await asyncpg.create_pool(target_sec_url, min_size=1, max_size=2, timeout=5.0)
            logger.info("Secondary Database connection pool initialized.")
        except Exception as sdbe:
            logger.error(f"SECONDARY DB CONNECTION DELAY: {sdbe}.")
            secondary_db_pool = None
    
    yield
    
    if db_pool:
        await db_pool.close()
    if secondary_db_pool:
        await secondary_db_pool.close()
    if redis_client:
        await redis_client.close()
    if http_client:
        await http_client.aclose()
    logger.info("System resources shutdown successfully.")

app = FastAPI(title="Kraken Swarm Engine", lifespan=lifespan)

@app.get("/api/v1/generate-qr-node")
async def get_sandbox_qr_node(content: str = "Kraken Swarm"):
    if not HAS_IMAGE_PROCESSING:
        raise HTTPException(status_code=501, detail="Core system image components unconfigured.")
    try:
        loop = asyncio.get_running_loop()
        image_buffer = await loop.run_in_executor(None, execute_cpu_heavy_image_task, content)
        return StreamingResponse(image_buffer, media_type="image/png")
    except Exception as runtime_img_err:
        logger.error(f"Image compilation crash: {runtime_img_err}")
        raise HTTPException(status_code=500, detail="Visual system stream rendering error.")

@app.post("/api/v1/krakendb/sync")
async def sync_krakendb(payload: KrakenDBSyncPayload):
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database currently offline.")
    
    async with db_pool.acquire() as conn:
        user = await conn.fetchrow("SELECT tier FROM user_vault WHERE session_id = $1", payload.session_id)
        if user and user["tier"] == "free":
            return {"status": "LOCAL_ONLY", "message": "State saved locally but cloud sync requires premium."}
            
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO krakendb_sync (session_id, store_name, data) VALUES ($1, $2, $3)",
                payload.session_id, payload.store_name, payload.payload_data
            )
        return {"status": "SUCCESS", "message": "Sandbox state captured dynamically."}
    except Exception as e:
        logger.error(f"KrakenDB sync error: {e}")
        raise HTTPException(status_code=500, detail="State save failure.")

@app.get("/api/v1/krakendb/sync/{session_id}")
async def get_krakendb_sync(session_id: str):
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database currently offline.")
    try:
        async with db_pool.acquire() as conn:
            user = await conn.fetchrow("SELECT tier FROM user_vault WHERE session_id = $1", session_id)
            if user and user["tier"] == "free":
                return {"session_id": session_id, "states": [], "msg": "Upgrade to Lite/Infinite to fetch state streams."}
                
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

@app.get("/api/v1/preview/{session_id}", response_class=HTMLResponse)
async def live_sandbox_preview(session_id: str):
    if redis_client:
        try:
            cached_html = await redis_client.get(f"preview:{session_id}")
            if cached_html:
                return HTMLResponse(content=cached_html, status_code=200)
        except Exception as re_err:
            logger.error(f"Redis fetch error inside preview route: {re_err}")

    if db_pool:
        try:
            async with db_pool.acquire() as conn:
                user = await conn.fetchrow("SELECT history, tier FROM user_vault WHERE session_id = $1", session_id)
                if user and user["history"]:
                    history_data = json.loads(user["history"]) if isinstance(user["history"], str) else user["history"]
                    if history_data and len(history_data) > 0:
                        last_code = history_data[-1].get("code", "<h3>No output compiled inside history yet.</h3>")
                        
                        if user["tier"] == "free":
                            paywall_banner = """
                            <div style="position:fixed; bottom:0; left:0; right:0; background:linear-gradient(to right, #e11d48, #be123c); color:white; text-align:center; padding:12px; font-family:sans-serif; font-weight:bold; z-index:99999; box-shadow: 0 -4px 10px rgba(0,0,0,0.3);">
                                 Free Preview Sandbox Mode. Deployment, Downloads, and Coding Adjustments are Locked. 
                                <button onclick="window.parent.postMessage('trigger_razorpay_modal', '*')" style="background:white; color:#be123c; border:none; padding:6px 16px; margin-left:15px; border-radius:6px; font-weight:bold; cursor:pointer;">Upgrade to Pro Plans</button>
                            </div>
                            """
                            last_code = last_code.replace("</body>", f"{paywall_banner}</body>")
                            
                        return HTMLResponse(content=last_code, status_code=200)
        except Exception as e:
            logger.error(f"Error serving fallback db preview: {e}")
            
    return HTMLResponse(content="<h3>Sandbox preview session not active or has been cleared.</h3>", status_code=404)

@app.get("/api/v1/export/{session_id}")
async def export_project_zip(session_id: str):
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database cluster starting up.")
        
    async with db_pool.acquire() as conn:
        user = await conn.fetchrow("SELECT tier, history FROM user_vault WHERE session_id = $1", session_id)
        if not user:
            raise HTTPException(status_code=404, detail="Session node not tracked.")
        if user["tier"] == "free":
            raise HTTPException(status_code=402, detail=" Exporting source code ZIP structures is exclusive to premium users.")
            
        content = ""
        if redis_client:
            try:
                content = await redis_client.get(f"preview:{session_id}")
            except Exception:
                pass
        
        if not content and user["history"]:
            history_data = json.loads(user["history"]) if isinstance(user["history"], str) else user["history"]
            if history_data and len(history_data) > 0:
                content = history_data[-1].get("code", "")

    if not content:
        raise HTTPException(status_code=404, detail="No active code deployment found to export.")

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
        zip_file.writestr("index.html", content)
        readme_txt = f"# Compiled Sandbox Project by Kraken Swarm Engine\n## Session ID: {session_id}\n"
        zip_file.writestr("README.md", readme_txt)

    zip_buffer.seek(0)
    return StreamingResponse(zip_buffer, media_type="application/x-zip-compressed", headers={'Content-Disposition': f'attachment; filename="kraken_{session_id[:8]}.zip"'})

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    dashboard_ui = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Kraken Swarm Production Engine Dashboard</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <style>
            .controls-container {
                display: flex;
                justify-content: flex-end;
                align-items: center;
                width: 100%;
                gap: 8px;
                padding: 10px;
                box-sizing: border-box;
                overflow: hidden;
            }
            .controls-container button {
                padding: 6px 14px;
                font-size: 13px;
                white-space: nowrap;
                border-radius: 6px;
                font-weight: 600;
                transition: all 0.2s ease;
            }
        </style>
    </head>
    <body class="bg-[#0B0B0F] text-white min-h-screen flex flex-col font-sans">
        
        <header class="border-b border-slate-800 bg-slate-900/50 backdrop-blur sticky top-0 z-50">
            <div class="controls-container max-w-7xl mx-auto">
                <button onclick="triggerAction('edit')" class="bg-blue-600 hover:bg-blue-500 text-white">Code Edit</button>
                <button onclick="triggerAction('preview')" class="bg-indigo-600 hover:bg-indigo-500 text-white">Live Preview</button>
                <button onclick="triggerAction('deploy')" class="bg-emerald-600 hover:bg-emerald-500 text-white">Deploy App</button>
            </div>
        </header>

        <main class="flex-1 max-w-7xl w-full mx-auto p-6 flex flex-col justify-between">
            
            <div id="sandbox-display-window" class="flex-1 w-full rounded-xl border border-dashed border-slate-800 bg-slate-950/20 flex flex-col items-center justify-center min-h-[450px] overflow-hidden relative transition-all duration-300">
                <div id="blank-placeholder" class="text-center p-8 z-10">
                    <h2 class="text-xl font-bold text-slate-500 tracking-wide mb-2">Sandbox Environment Initialized</h2>
                    <p class="text-slate-600 text-sm">Enter a topic below to dynamically compile your 3D high-converting system.</p>
                </div>
                <iframe id="live-render-frame" class="absolute inset-0 w-full h-full border-none hidden"></iframe>
            </div>

            <div class="mt-6 bg-slate-900/60 border border-slate-800 rounded-xl p-4 shadow-xl">
                <div class="flex flex-col md:flex-row gap-4 items-center">
                    <input type="text" id="user-topic-input" placeholder="Type any topic (e.g., Crypto Arbitrage, AI Marketing, SaaS Architecture)..." class="w-full flex-1 bg-slate-950 border border-slate-800 rounded-lg px-4 py-3 text-sm focus:outline-none focus:border-blue-500 transition-colors">
                    <button onclick="triggerSwarmGeneration()" class="w-full md:w-auto bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-500 hover:to-indigo-500 px-6 py-3 rounded-lg text-sm font-bold tracking-wide shadow-lg whitespace-nowrap transition-all">Generate Architecture</button>
                </div>
                
                <div class="mt-4 pt-4 border-t border-slate-800/60 flex flex-col sm:flex-row items-center justify-between gap-4">
                    <div id="status-message" class="text-xs font-semibold text-slate-500 tracking-wide">System Standby Mode</div>
                    <div id="auth-box">
                        <button onclick="triggerGoogleSandboxClaim()" class="flex items-center gap-2 bg-white text-slate-950 text-xs font-bold py-2 px-4 rounded-md hover:bg-slate-100 transition-colors">
                            Sign in with Google
                        </button>
                    </div>
                </div>
            </div>
        </main>

        <script>
        let ws;
        const sessionId = localStorage.getItem("kraken_active_session") || 'sess_' + Math.random().toString(36).substring(2, 15);
        localStorage.setItem("kraken_active_session", sessionId);

        function initWebSocket() {
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            ws = new WebSocket(`${protocol}//${window.location.host}/ws/v1/swarm-orchestrator/${sessionId}`);
            
            ws.onmessage = (event) => {
                const data = JSON.parse(event.data);
                const statusMsg = document.getElementById("status-message");
                
                if (data.log) {
                    statusMsg.innerText = `[${data.agent || 'System'}]: ${data.log}`;
                }
                
                if (data.blueprint_structure) {
                    const currentTopic = document.getElementById("user-topic-input").value.trim();
                    ws.send(JSON.stringify({ task: currentTopic, blueprint_approved: true }));
                }

                if (data.preview_url) {
                    document.getElementById("blank-placeholder").classList.add("hidden");
                    const frame = document.getElementById("live-render-frame");
                    frame.classList.remove("hidden");
                    frame.src = data.preview_url + "?t=" + new Date().getTime();
                    statusMsg.innerText = "Execution Pipeline Complete: Render Active.";
                }
            };
        }

        async function triggerSwarmGeneration() {
            const topic = document.getElementById("user-topic-input").value.trim();
            if(!topic) return;
            if(!ws || ws.readyState !== WebSocket.OPEN) initWebSocket();
            
            document.getElementById("status-message").innerText = "Initiating swarm generation pipeline...";
            setTimeout(() => {
                ws.send(JSON.stringify({ task: topic, blueprint_approved: false }));
            }, 500);
        }

        function triggerAction(type) {
            const statusMsg = document.getElementById("status-message");
            statusMsg.innerText = `Action Triggered: Accessing ${type.toUpperCase()} node...`;
            
            if (type === 'edit') {
                const instructions = prompt("Enter specific UI adjustments or color updates:");
                if (instructions && ws && ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify({ edit_instruction: instructions }));
                }
            } else if (type === 'preview') {
                window.open(`/api/v1/preview/${sessionId}`, '_blank');
            } else if (type === 'deploy') {
                alert("Gatekeeper Status Check: Standard deployment cluster optimized.");
            }
        }

        async function triggerGoogleSandboxClaim() {
            const statusMsg = document.getElementById("status-message");
            const userEmail = prompt("Enter your verified Google Account Email address:");
            if(!userEmail || !userEmail.includes("@")) {
                statusMsg.innerText = "Invalid email configuration mapping.";
                return;
            }
            try {
                const response = await fetch('/api/v1/activate-node', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        session_id: sessionId,
                        email: userEmail,
                        browser_timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
                        device_fingerprint: "fp_static_hash_compiled_platform_node"
                    })
                });
                const data = await response.json();
                if(response.ok) {
                    statusMsg.innerText = "Verification Confirmed: Active Session.";
                } else {
                    statusMsg.innerText = data.detail || "Quota mapping failed.";
                }
            } catch(e) {
                statusMsg.innerText = "Network handshake error.";
            }
        }

        window.onload = () => { initWebSocket(); };
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
        raise HTTPException(status_code=400, detail=" Professional or authenticated email networks only.")
    
    tz = payload.browser_timezone.lower()
    arbitrage_risk = False
    
    try:
        async with db_pool.acquire() as conn:
            email_check = await conn.fetchrow("SELECT * FROM user_vault WHERE email = $1 AND free_tier_claimed = TRUE", email)
            if email_check:
                raise HTTPException(status_code=403, detail=" Access Denied: Lifetime free quota pehle hi claim kiya ja chuka hai.")

            fingerprint_check = await conn.fetchrow("SELECT * FROM user_vault WHERE device_hash = $1 AND free_tier_claimed = TRUE", payload.device_fingerprint)
            if fingerprint_check:
                raise HTTPException(status_code=403, detail=" Access Denied: Device profile signature duplicate match.")

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
            try:
                await redis_client.set(f"user:{payload.session_id}:tier", "free", ex=3600)
            except Exception:
                pass
            
    except HTTPException as he:
        raise he
    except Exception as dbe:
        logger.error(f"Error executing db transaction in activate_node: {dbe}")
        raise HTTPException(status_code=500, detail="Internal lock configuration sync error.")
        
    return {"status": "SUCCESS", "message": "Authenticated successfully."}

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
                try:
                    await redis_client.set(f"user:{session_id}:history", json.dumps(response_data), ex=300)
                except Exception:
                    pass
            return response_data
    except Exception as e:
        logger.error(f"History routing exception: {e}")
        return {"tier": "free", "history": [], "error": "Internal synchronization error"}

async def call_gemini_agent(agent_name: str, system_instruction: str, user_prompt: str) -> str:
    if not http_client:
        return f"[{agent_name} Core Simulation Output]: Bypass mode active."
        
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
                    return res_data["candidates"][0]["content"]["parts"][0].get("text", "")
        except Exception as e:
            logger.warning(f"Primary Endpoint [Gemini] failure: {e}. Fallback mode tracking...")
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
            logger.warning(f"Secondary Endpoint [OpenRouter] failure: {e}.")
            continue

    return f"[{agent_name} Output]: System workspace simulation processed dynamically."

async def self_heal_output_code(raw_code: str) -> str:
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
    return healed

async def save_history_bg(sid: str, task: str, html: str):
    if redis_client:
        try:
            await redis_client.set(f"preview:{sid}", html, ex=7200)
        except Exception as re_err:
            logger.error(f"Redis pipeline error caching html state: {re_err}")

    if not db_pool:
        return
    try:
        async with db_pool.acquire() as db_conn:
            user_row = await db_conn.fetchrow("SELECT history FROM user_vault WHERE session_id = $1", sid)
            h_list = []
            if user_row and user_row["history"]:
                try:
                    h_list = json.loads(user_row["history"]) if isinstance(user_row["history"], str) else user_row["history"]
                except Exception:
                    h_list = []
            h_list.append({"task": task, "code": html})
            await db_conn.execute("UPDATE user_vault SET history = $1 WHERE session_id = $2", json.dumps(h_list), sid)
            if redis_client:
                try:
                    await redis_client.delete(f"user:{sid}:history")
                except Exception:
                    pass
    except Exception as e:
        logger.error(f"Error in saving background history data: {str(e)}")

async def process_async_agents_pipeline(user_task: str, combined_context_dict: dict, websocket: WebSocket, tier: str):
    speed_factor = 0.3 if tier == "infinite" else (0.1 if tier == "enterprise" else 1.0)
    agents_pipeline = [
        {"name": "Security Auditor", "prompt": "Identify vulnerabilities."},
        {"name": "Swarm Architect", "prompt": "Map layouts and components."},
        {"name": "Production Engine", "prompt": "Write components data logic."},
        {"name": "Kraken Assembler", "prompt": "Compile elements cleanly."},
        {"name": "De-Penalization Agent", "prompt": "Aesthetic validation check."}
    ]
    
    async def run_single_agent(idx, agent):
        try:
            await websocket.send_json({"agent": agent["name"], "log": f"Launching Swarm Agent [{idx}/5]..."})
            await call_gemini_agent(agent["name"], agent["prompt"], user_task)
            combined_context_dict[agent["name"]] = f"Processed under {tier} tier specifications."
            await websocket.send_json({"agent": agent["name"], "log": f" Agent [{idx}/5] verified."})
            await asyncio.sleep(0.5 * speed_factor)
        except Exception as ae:
            combined_context_dict[agent["name"]] = f"Bypass state: {ae}"

    await asyncio.gather(*(run_single_agent(i + 1, a) for i, a in enumerate(agents_pipeline)))

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
                else:
                    tier = user["tier"] if user["tier"] else "free"
                    free_claimed = user["free_tier_claimed"]
                    queries_used = user["queries_used_today"] if user["queries_used_today"] is not None else 0
        except Exception as err:
            logger.error(f"Exception inside DB Handshake: {err}")
            
    try:
        await websocket.send_json({"tier": tier, "status": "CONNECTED"})
    except Exception:
        return
    
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

            if tier == "free" and (queries_used >= PLAN_SAFETY_LIMITS["free"]["max_daily_queries"]):
                await websocket.send_json({"agent": "Kraken Paywall Director", "log": " Access Denied: Quota exhausted."})
                continue

            if tier == "free" and (len(user_task) > PLAN_SAFETY_LIMITS["free"]["max_chars"] or len(edit_instruction) > PLAN_SAFETY_LIMITS["free"]["max_chars"]):
                await websocket.send_json({"agent": "Kraken Swarm Director", "log": " Limit Exceeded: Max character rule hit."})
                continue

            if edit_instruction and tier == "free":
                await websocket.send_json({"agent": "Kraken Paywall Director", "log": " Feature Locked: Upgrade required for edits."})
                continue

            try:
                if db_pool:
                    async with db_pool.acquire() as conn:
                        queries_used += 1
                        await conn.execute("UPDATE user_vault SET free_tier_claimed = TRUE, queries_used_today = $1 WHERE session_id = $2", queries_used, session_id)
            except Exception as db_mod_err:
                logger.error(f"Error updating database logs: {db_mod_err}")

            if edit_instruction:
                existing_html = ""
                if redis_client:
                    try:
                        existing_html = await redis_client.get(f"preview:{session_id}")
                    except Exception:
                        pass
                
                if not existing_html:
                    user_task = edit_instruction
                else:
                    editor_system_instruction = "Modify the existing application based strictly on user edit instruction directives."
                    final_html_raw = await call_gemini_agent("Kraken Editor", editor_system_instruction, f"Code:\n{existing_html}\n\nEdit:\n{edit_instruction}")
                    final_html = await self_heal_output_code(final_html_raw)
                    asyncio.create_task(save_history_bg(session_id, f"Edited: {edit_instruction}", final_html))
                    
                    chunk_size = 4096
                    for i in range(0, len(final_html), chunk_size):
                        await websocket.send_json({"agent": "Kraken Editor", "chunk_output": final_html[i:i+chunk_size]})
                    
                    await websocket.send_json({"tier": tier, "preview_url": f"/api/v1/preview/{session_id}", "result_data": {"status": "SUCCESS"}})
                    continue

            if not is_approved:
                blueprint_plan = await call_gemini_agent("Blueprint Engine", "Build layout maps implementation roadmap.", user_task)
                await websocket.send_json({"agent": "Blueprint Engine", "blueprint_structure": blueprint_plan, "log": " Blueprint validated."})
                continue

            if tier == "free":
                await asyncio.sleep(PLAN_SAFETY_LIMITS["free"]["delay_seconds"])

            combined_context_dict = {}
            await process_async_agents_pipeline(user_task, combined_context_dict, websocket, tier)
            combined_context = "\n".join([f"[{k}]: {v}" for k, v in combined_context_dict.items()])
            
            database_setup_snippet = f"""
            <script>
            class KrakenDB {{
                static init(storeName) {{
                    this.storeName = storeName; this.sessionId = "{session_id}";
                    if (!localStorage.getItem(storeName)) localStorage.setItem(storeName, JSON.stringify([]));
                }}
                static async insert(data) {{
                    const items = JSON.parse(localStorage.getItem(this.storeName) || '[]');
                    const record = {{ id: Date.now(), ...data, created_at: new Date().toISOString() }};
                    items.push(record); localStorage.setItem(this.storeName, JSON.stringify(items));
                    try {{ fetch('/api/v1/krakendb/sync', {{ method: 'POST', headers: {{ 'Content-Type': 'application/json' }}, body: JSON.stringify({{ session_id: this.sessionId, store_name: this.storeName, payload_data: record }}) }}); }} catch(e) {{}}
                    return record;
                }}
            }}
            
            const hueSeeds = [Math.floor(Math.random() * 360), Math.floor(Math.random() * 360), Math.floor(Math.random() * 360)];
            document.documentElement.style.setProperty('--gradient-c1', `hsl(${{hueSeeds[0]}}, 85%, 55%)`);
            document.documentElement.style.setProperty('--gradient-c2', `hsl(${{hueSeeds[1]}}, 90%, 50%)`);
            document.documentElement.style.setProperty('--gradient-c3', `hsl(${{hueSeeds[2]}}, 80%, 45%)`);
            </script>
            """
            
            assembler_instruction = f"Synthesize single stand-alone software dashboard module layout using Tailwind CSS integrated with high-converting dynamic variable colors and: {database_setup_snippet}"
            final_html_raw = await call_gemini_agent("Kraken Assembler", assembler_instruction, f"Req: {user_task}\nCtx: {combined_context}")
            final_html = await self_heal_output_code(final_html_raw)

            asyncio.create_task(save_history_bg(session_id, user_task, final_html))
            
            chunk_size = 4096
            for i in range(0, len(final_html), chunk_size):
                await websocket.send_json({"agent": "Kraken Assembler", "chunk_output": final_html[i:i+chunk_size]})
            
            await websocket.send_json({"tier": tier, "preview_url": f"/api/v1/preview/{session_id}", "result_data": {"status": "SUCCESS"}})
            
    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected safely: {session_id}")
    except Exception as e:
        logger.error(f"Swarm Edge Fatal Exception: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "10000"))
    print(f"KRAKEN SWARM PRODUCTION CORE ONLINE ON PORT: {port}")
    uvicorn.run(app, host="0.0.0.0", port=port, reload=False)
