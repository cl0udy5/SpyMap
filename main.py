import os
import logging
import time
import threading
import asyncio
import stripe as stripe_module
import paypalrestsdk as paypal
from dotenv import load_dotenv
from flask import Flask, request, Response
from waitress import serve

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
    PicklePersistence,
)

# --- Global variables for async loop & thread ---
ASYNC_LOOP = asyncio.new_event_loop()
async_thread = threading.Thread(target=ASYNC_LOOP.run_forever, daemon=True)

from maps_scraper import collect_leads, geocode_location, write_csv, write_excel

# --- Load Environment Variables & Configure ---
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
WEBHOOK_URL = os.getenv("WEBHOOK_URL") # e.g., https://your-domain.com
PORT = int(os.getenv("PORT", "8443"))

SEND_BOTH_FORMATS = False

# --- Stripe Configuration ---
# Default to "test" mode for safety. Set STRIPE_MODE=live in .env for real payments.
STRIPE_MODE = os.getenv("STRIPE_MODE", "test")

if STRIPE_MODE == "live":
    STRIPE_API_KEY = os.getenv("STRIPE_API_KEY_LIVE")
    STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET_LIVE")
else:
    STRIPE_API_KEY = os.getenv("STRIPE_API_KEY_TEST")
    STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET_TEST")

# Configure Stripe
stripe_module.api_key = STRIPE_API_KEY

# --- PayPal Configuration ---
PAYPAL_MODE = os.getenv("PAYPAL_MODE", "sandbox")

if PAYPAL_MODE == "live":
    PAYPAL_CLIENT_ID = os.getenv("PAYPAL_CLIENT_ID_LIVE")
    PAYPAL_CLIENT_SECRET = os.getenv("PAYPAL_CLIENT_SECRET_LIVE")
else:
    PAYPAL_CLIENT_ID = os.getenv("PAYPAL_CLIENT_ID_SANDBOX")
    PAYPAL_CLIENT_SECRET = os.getenv("PAYPAL_CLIENT_SECRET_SANDBOX")

PAYPAL_WEBHOOK_ID = os.getenv("PAYPAL_WEBHOOK_ID")

# Configure PayPal SDK
paypal.configure({
    "mode": PAYPAL_MODE,  # sandbox or live
    "client_id": PAYPAL_CLIENT_ID,
    "client_secret": PAYPAL_CLIENT_SECRET
})


# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Add a log message to make the current mode clear on startup
if STRIPE_MODE == "live":
    logger.info("--- STRIPE IS IN LIVE MODE --- REAL PAYMENTS WILL BE PROCESSED ---")
else:
    logger.info("--- Stripe is in TEST MODE. No real payments will be processed. ---")


# --- Global App State ---
ptb_app: Application | None = None
bot_initialized = False
bot_init_lock = threading.Lock()


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log Errors caused by Updates."""
    logger.error(msg="Exception while handling an update:", exc_info=context.error)

# --- Pricing Calculation (Unchanged) ---
PRICE_GEOCODING_PER_1000 = 5.00
PRICE_NEARBY_SEARCH_PER_1000 = 32.00
PRICE_PLACE_DETAILS_PER_1000 = 17.00
ESTIMATED_PAGES_PER_KEYWORD = 3
ESTIMATED_PLACES_PER_PAGE = 18
ESTIMATED_AWS_COST_PER_JOB = 0.10

def calculate_price(context: ContextTypes.DEFAULT_TYPE) -> float:
    data = context.user_data
    num_keywords = len(data.get("keywords", []))
    location = data.get("location", "")
    gmaps_cost = 0.0
    if "," not in location:
        gmaps_cost += PRICE_GEOCODING_PER_1000 / 1000
    num_nearby_searches = num_keywords * ESTIMATED_PAGES_PER_KEYWORD
    gmaps_cost += (num_nearby_searches / 1000) * PRICE_NEARBY_SEARCH_PER_1000
    num_place_details = num_keywords * ESTIMATED_PAGES_PER_KEYWORD * ESTIMATED_PLACES_PER_PAGE
    gmaps_cost += (num_place_details / 1000) * PRICE_PLACE_DETAILS_PER_1000
    total_internal_cost = gmaps_cost + ESTIMATED_AWS_COST_PER_JOB
    user_price = total_internal_cost * 2.0
    return max(round(user_price, 2), 0.50)

# --- Conversation Steps & Handlers (Mostly Unchanged) ---
STEP_LOCATION = "location"
STEP_RADIUS = "radius"
STEP_KEYWORDS = "keywords"
STEP_FILTERS = "filters"

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.clear()
    msg = await update.message.reply_text(
        "🚀 Welcome to the SpyMap Bot! 🚀\n\n"
        "I can help you find business leads from Google Maps. Just give me a location, a search radius, and some keywords, and I'll generate a CSV file with the results for you.\n\n"
        "To get started, please send me either a city name (e.g. “Berlin”) or coordinates (e.g. “52.5200,13.4050”) to search around.\n\n"
        "P.S. If you get stuck, or want to restart the bot, just type /start again.",
    )
    context.user_data["bot_msg_id"] = msg.message_id
    context.user_data["step"] = STEP_LOCATION

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_step = context.user_data.get("step")
    user_text = update.message.text.strip()
    bot_msg_id = context.user_data.get("bot_msg_id")
    await update.message.delete()

    if user_step == STEP_LOCATION:
        context.user_data["location"] = user_text
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id, message_id=bot_msg_id,
            text=f"📍 Location set to: _{user_text}_", parse_mode="Markdown",
        )
        msg = await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="✨ Great! Now send me the search radius in meters (e.g. 100-50000)."
        )
        context.user_data["bot_msg_id"] = msg.message_id
        context.user_data["step"] = STEP_RADIUS

    elif user_step == STEP_RADIUS:
        if not user_text.isdigit():
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="❌ Please send a number for the radius (e.g. 100-50000)."
            )
            return
        context.user_data["radius"] = int(user_text)
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id, message_id=bot_msg_id,
            text=f"📏 Radius set to: *{user_text}* meters", parse_mode="Markdown",
        )
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("✅ Done", callback_data="keywords_done")]])
        msg = await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="🔑 Awesome! Now send me your keywords, one by one. Press 'Done' when you have added all of them.",
            reply_markup=keyboard,
        )
        context.user_data["bot_msg_id"] = msg.message_id
        context.user_data["step"] = STEP_KEYWORDS
        context.user_data["keywords"] = []

    elif user_step == STEP_KEYWORDS:
        keywords = context.user_data.get("keywords", [])
        keywords.append(user_text)
        context.user_data["keywords"] = keywords
        keyword_list = "\n".join(f"- `{k}`" for k in keywords)
        summary = (
            f"Your keywords so far:\n{keyword_list}\n\n"
            f"Send another keyword, or press 'Done' when finished."
        )
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("✅ Done", callback_data="keywords_done")]])
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id, message_id=bot_msg_id,
            text=summary, parse_mode="Markdown", reply_markup=keyboard,
        )
    else:
        await context.bot.send_message(
            chat_id=update.effective_chat.id, text="Type /start to begin a new search."
        )

async def show_filter_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    filters = context.user_data.get("filters", {})
    website_filter = filters.get("website", "no_filter").replace("_", " ").title()
    email_filter = filters.get("email", "no_filter").replace("_", " ").title()
    rating_filter_val = filters.get("rating", "no_filter")
    rating_filter_text = "No Filter" if rating_filter_val == "no_filter" else ( "5" if rating_filter_val == "5" else f"{rating_filter_val}+")
    summary = (
        f"✅ All set!\n\n"
        f"*Location:* {context.user_data['location']}\n"
        f"*Radius:* {context.user_data['radius']} m\n"
        f"*Keywords:* {', '.join(context.user_data['keywords'])}\n\n"
        f"--- *Filters* ---\n"
        f"🌐 Website: *{website_filter}*\n"
        f"📧 Email: *{email_filter}*\n"
        f"⭐ Rating: *{rating_filter_text}*\n"
    )
    keyboard = [
        [
            InlineKeyboardButton("🌐 Website", callback_data="filter_website"),
            InlineKeyboardButton("📧 Email", callback_data="filter_email"),
            InlineKeyboardButton("⭐ Rating", callback_data="filter_rating"),
        ],
        [InlineKeyboardButton("🚀 Start Scraping", callback_data="start_scraping")],
    ]
    if query:
        await query.edit_message_text(
            text=summary, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard)
        )

async def handle_filter_selection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    filter_type = query.data.split("_")[1]
    text = f"Select filter for *{filter_type.title()}*:"
    if filter_type == "website":
        keyboard = [
            [InlineKeyboardButton("With Website", callback_data="set_filter_website_with"), InlineKeyboardButton("Without Website", callback_data="set_filter_website_without")],
            [InlineKeyboardButton("No Filter", callback_data="set_filter_website_no_filter")],
            [InlineKeyboardButton("⬅️ Back", callback_data="back_to_filters")],
        ]
    elif filter_type == "email":
        keyboard = [
            [InlineKeyboardButton("With Email", callback_data="set_filter_email_with"), InlineKeyboardButton("Without Email", callback_data="set_filter_email_without")],
            [InlineKeyboardButton("No Filter", callback_data="set_filter_email_no_filter")],
            [InlineKeyboardButton("⬅️ Back", callback_data="back_to_filters")],
        ]
    elif filter_type == "rating":
        keyboard = [
            [InlineKeyboardButton("1+", callback_data="set_filter_rating_1"), InlineKeyboardButton("2+", callback_data="set_filter_rating_2"), InlineKeyboardButton("3+", callback_data="set_filter_rating_3")],
            [InlineKeyboardButton("4+", callback_data="set_filter_rating_4"), InlineKeyboardButton("5", callback_data="set_filter_rating_5")],
            [InlineKeyboardButton("No Filter", callback_data="set_filter_rating_no_filter")],
            [InlineKeyboardButton("⬅️ Back", callback_data="back_to_filters")],
        ]
    else: return
    await query.edit_message_text(text=text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def handle_set_filter(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    parts = query.data.split("_")
    filter_type, filter_value = parts[2], "_".join(parts[3:])
    if "filters" not in context.user_data:
        context.user_data["filters"] = {}
    context.user_data["filters"][filter_type] = filter_value
    await show_filter_menu(update, context)

async def handle_back_to_filters(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    await show_filter_menu(update, context)

async def handle_keywords_done(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    keywords = context.user_data.get("keywords", [])
    if not keywords:
        await query.answer("Please add at least one keyword first!", show_alert=True)
        return
    context.user_data["filters"] = {"website": "no_filter", "email": "no_filter", "rating": "no_filter"}
    context.user_data["step"] = STEP_FILTERS
    await show_filter_menu(update, context)

async def handle_start_scraping_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the Start Scraping button: calculate price and show payment options."""
    query = update.callback_query
    await query.answer()
    price = calculate_price(context)

    # Check which payment providers are configured
    stripe_configured = all([STRIPE_API_KEY, STRIPE_WEBHOOK_SECRET])
    paypal_configured = all([PAYPAL_CLIENT_ID, PAYPAL_CLIENT_SECRET, PAYPAL_WEBHOOK_ID])

    payment_buttons = []
    if stripe_configured:
        payment_buttons.append(InlineKeyboardButton("🚀 Pay with Stripe", callback_data="pay_stripe"))
    # if paypal_configured:
    #     payment_buttons.append(InlineKeyboardButton("Pay with PayPal", callback_data="pay_paypal"))

    if not payment_buttons:
        await query.edit_message_text("❌ Payment processing is not configured. Please contact the administrator.")
        logger.error("No payment provider is configured.")
        return

    keyboard = InlineKeyboardMarkup([payment_buttons]) # This will put buttons on the same row

    await query.edit_message_text(
        f"💰 Before the scraping starts, a small processing fee is required. Based on the input parameters, the fee is *${price:.2f}*.\n\n"
        "Please choose your payment method below to proceed.",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )

async def handle_stripe_pay_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the Pay button: create a Stripe Checkout session and send the link."""

    query = update.callback_query
    await query.answer()
    chat_id = query.message.chat_id
    price = calculate_price(context)
    price_in_cents = int(price * 100)

    # --- DEBUG: Log the type and value of stripe_module.checkout and Session ---
    logger.info(f"stripe_module: {stripe_module}")
    logger.info(f"stripe_module.__file__: {getattr(stripe_module, '__file__', 'N/A')}")
    logger.info(f"stripe_module.checkout: {getattr(stripe_module, 'checkout', None)} (type: {type(getattr(stripe_module, 'checkout', None))})")
    if hasattr(stripe_module, 'checkout'):
        logger.info(f"stripe_module.checkout.Session: {getattr(stripe_module.checkout, 'Session', None)} (type: {type(getattr(stripe_module.checkout, 'Session', None))})")

    if not STRIPE_API_KEY:
        await query.edit_message_text("❌ Payment processing is not configured.")
        logger.error("STRIPE_API_KEY is not set.")
        return

    try:
        # Define the blocking function for Stripe
        def create_stripe_session():
            return stripe_module.checkout.Session.create(
                line_items=[{
                    'price_data': {
                        'currency': 'usd',
                        'product_data': {'name': 'SpyMap Scraping Job'},
                        'unit_amount': price_in_cents,
                    },
                    'quantity': 1,
                }],
                mode='payment',
                success_url=f"{WEBHOOK_URL}/success", # Placeholder, real confirmation is via webhook
                cancel_url=f"{WEBHOOK_URL}/cancel",
                metadata={'chat_id': chat_id} # Pass chat_id to identify user later
            )

        # Run the blocking call in the default thread pool executor
        loop = asyncio.get_running_loop()
        checkout_session = await loop.run_in_executor(None, create_stripe_session)
        
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("Pay with Stripe", url=checkout_session.url)]])
        await query.edit_message_text(
            f"💰 Before the scraping starts, a small processing fee is required. Based on the input parameters, the fee is *${price:.2f}*.\n\n"
            "Click the button below to proceed with the payment.",
            reply_markup=keyboard,
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"Stripe session creation failed: {e}")
        await query.edit_message_text("❌ Could not create a payment session. Please try again later.")


async def handle_paypal_pay_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the PayPal Pay button: create a PayPal payment and send the link."""
    query = update.callback_query
    await query.answer()
    chat_id = query.message.chat_id
    price = calculate_price(context)

    if not all([PAYPAL_CLIENT_ID, PAYPAL_CLIENT_SECRET]):
        await query.edit_message_text("❌ PayPal processing is not configured.")
        logger.error("PAYPAL_CLIENT_ID or PAYPAL_CLIENT_SECRET is not set.")
        return

    # Define the blocking function for PayPal
    def create_paypal_payment():
        payment = paypal.Payment({
            "intent": "sale",
            "payer": {
                "payment_method": "paypal"
            },
            "redirect_urls": {
                "return_url": f"{WEBHOOK_URL}/success",
                "cancel_url": f"{WEBHOOK_URL}/cancel"
            },
            "transactions": [{
                "item_list": {
                    "items": [{
                        "name": "SpyMap Scraping Job",
                        "sku": "SM-JOB-01",
                        "price": f"{price:.2f}",
                        "currency": "USD",
                        "quantity": 1
                    }]
                },
                "amount": {
                    "total": f"{price:.2f}",
                    "currency": "USD"
                },
                "description": "Payment for a Google Maps scraping job.",
                "custom": str(chat_id) # Pass chat_id to identify user later
            }]
        })

        if payment.create():
            return payment
        else:
            logger.error(f"PayPal payment creation failed: {payment.error}")
            return None

    try:
        # Run the blocking call in the default thread pool executor
        loop = asyncio.get_running_loop()
        payment = await loop.run_in_executor(None, create_paypal_payment)

        if payment:
            approval_url = next(link.href for link in payment.links if link.rel == "approval_url")
            keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("Pay with PayPal", url=approval_url)]])
            await query.edit_message_text(
                f"💰 The calculated price for your job is *${price:.2f}*.\n\n"
                "Click the button below to proceed with the payment via PayPal.",
                reply_markup=keyboard,
                parse_mode="Markdown"
            )
        else:
            await query.edit_message_text("❌ Could not create a PayPal payment session. Please try again later.")

    except Exception as e:
        logger.error(f"PayPal session creation failed: {e}", exc_info=True)
        await query.edit_message_text("❌ Could not create a payment session. Please try again later.")


async def execute_scraping(bot, chat_id: int, user_data: dict) -> None:
    """
    The actual scraping logic. Runs blocking I/O in a thread pool to avoid
    blocking the asyncio event loop.
    """
    location = user_data.get("location")
    radius = user_data.get("radius")
    keywords = user_data.get("keywords")
    filters = user_data.get("filters", {})
    loop = asyncio.get_running_loop()

    try:
        await bot.send_message(
            chat_id=chat_id,
            text="✅ Payment successful! Your scraping job is starting now. This may take several minutes depending on the number of keywords..."
        )

        # --- This function contains all the blocking I/O ---
        def _blocking_scraping_and_file_io():
            center = geocode_location(location, GOOGLE_API_KEY) if "," not in location else location
            leads = collect_leads(center, radius, keywords, GOOGLE_API_KEY, filters)

            if not leads:
                return None, None, None

            base_filename = f"leads_{chat_id}_{int(time.time())}"
            csv_path = f"{base_filename}.csv"
            write_csv(leads, csv_path)

            excel_path = None
            if SEND_BOTH_FORMATS:
                excel_path = f"{base_filename}.xlsx"
                write_excel(leads, excel_path)
            
            return leads, csv_path, excel_path

        # --- Run the blocking code in the default thread pool executor ---
        leads, csv_path, excel_path = await loop.run_in_executor(
            None, _blocking_scraping_and_file_io
        )

        # --- Now back in the async context to send files ---
        if not leads:
            await bot.send_message(chat_id=chat_id, text="❌ No results found for your search criteria.")
            return

        caption = f"✅ Found {len(leads)} unique results for: {', '.join(keywords)}"

        if csv_path:
            try:
                with open(csv_path, "rb") as f_csv:
                    await bot.send_document(
                        chat_id=chat_id, document=f_csv, caption=caption,
                        filename=f"leads_{'-'.join(keywords)}.csv"
                    )
            finally:
                os.remove(csv_path)

        if excel_path:
            try:
                with open(excel_path, "rb") as f_excel:
                    await bot.send_document(
                        chat_id=chat_id, document=f_excel, # No caption on second file
                        filename=f"leads_{'-'.join(keywords)}.xlsx"
                    )
            finally:
                os.remove(excel_path)

        final_message = "✅ Both CSV and Excel files sent." if SEND_BOTH_FORMATS else "✅ Your CSV file has been sent."
        await bot.send_message(chat_id=chat_id, text=f"{final_message} Type /start for a new search.")

    except Exception as e:
        logger.error(f"Error during scraping for chat {chat_id}: {e}", exc_info=True)
        # Provide a user-friendly error message instead of the raw exception
        await bot.send_message(
            chat_id=chat_id,
            text="❌ An unexpected error occurred while processing your request. "
                 "The technical team has been notified. Please try again later or contact support."
        )

def send_telegram_message_sync(chat_id: int, text: str):
    """
    Sends a Telegram message from a synchronous context by submitting it
    to the running asyncio event loop.
    """
    if not ptb_app:
        logger.error("Cannot send message from sync context: ptb_app not initialized.")
        return

    future = asyncio.run_coroutine_threadsafe(
        ptb_app.bot.send_message(chat_id=chat_id, text=text), ASYNC_LOOP
    )
    try:
        future.result(timeout=15)
        logger.info(f"Sync message sent to {chat_id}")
    except Exception as e:
        logger.error(f"Failed to send sync message to {chat_id}: {e}", exc_info=True)


# --- WEBHOOK SERVER & APP SETUP ---
flask_app = Flask(__name__)


@flask_app.route("/stripe", methods=['POST'])
def stripe_webhook():
    """Webhook endpoint for Stripe to confirm payments."""
    payload = request.data
    sig_header = request.headers.get('Stripe-Signature')
    event = None

    try:
        event = stripe_module.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
    except ValueError as e: # Invalid payload
        logger.error(f"Stripe webhook ValueError: {e}")
        return Response(status=400)
    except stripe_module.error.SignatureVerificationError as e: # Invalid signature
        logger.error(f"Stripe signature verification error: {e}")
        return Response(status=400)

    # Handle the checkout.session.completed event
    if event['type'] == 'checkout.session.completed':
        session = event['data']['object']
        chat_id = int(session['metadata']['chat_id'])
        
        if not ptb_app:
            logger.error("ptb_app not initialized in stripe_webhook!")
            return Response(status=500)
            
        user_data = ptb_app.user_data.get(chat_id, {})

        if not user_data:
             logger.warning(f"Could not find user_data for chat_id {chat_id} after payment.")
             send_telegram_message_sync(
                 chat_id,
                 "✅ Payment received! However, I couldn't find your session data. "
                 "This can happen if the bot was restarted recently. "
                 "Please type /start to begin a new search."
             )
             return Response(status=200)

        logger.info(f"Payment successful for chat_id: {chat_id}. Starting scrape.")
        
        # Run scraping in the background on the shared event loop to not block the webhook response
        asyncio.run_coroutine_threadsafe(
            execute_scraping(ptb_app.bot, chat_id, user_data.copy()), ASYNC_LOOP
        )

    return Response(status=200)

@flask_app.route("/paypal-webhook", methods=['POST'])
def paypal_webhook():
    """Webhook endpoint for PayPal to confirm payments."""
    try:
        # The library verifies the webhook signature for you
        event = paypal.WebhookEvent.verify(
            request.headers.get('Paypal-Transmission-Id'),
            request.headers.get('Paypal-Transmission-Time'),
            PAYPAL_WEBHOOK_ID,
            request.data.decode('utf-8')
        )

        if event.event_type == "PAYMENT.SALE.COMPLETED":
            sale = event.resource
            custom_field = sale.get("custom")
            if not custom_field:
                logger.error("PayPal webhook received but no custom field (chat_id) found.")
                return Response(status=200)

            chat_id = int(custom_field)
            logger.info(f"PayPal payment successful for chat_id: {chat_id}. Starting scrape.")

            if not ptb_app:
                logger.error("ptb_app not initialized in paypal_webhook!")
                return Response(status=500)

            user_data = ptb_app.user_data.get(chat_id, {})
            if not user_data:
                logger.warning(f"Could not find user_data for chat_id {chat_id} after PayPal payment.")
                send_telegram_message_sync(
                    chat_id,
                    "✅ Payment received! However, I couldn't find your session data. Please type /start to begin a new search."
                )
                return Response(status=200)

            # Run scraping in the background
            asyncio.run_coroutine_threadsafe(
                execute_scraping(ptb_app.bot, chat_id, user_data.copy()), ASYNC_LOOP
            )

    except Exception as e:
        logger.error(f"Error processing PayPal webhook: {e}", exc_info=True)
        return Response(status=400) # Return bad request on error

    return Response(status=200)


@flask_app.route("/telegram", methods=['POST'])
def telegram_webhook():
    """Webhook endpoint for Telegram to receive updates."""
    if not ptb_app:
        logger.error("ptb_app not initialized in telegram_webhook!")
        return Response(status=500)
    update_data = request.get_json()
    update = Update.de_json(update_data, ptb_app.bot)
    asyncio.run_coroutine_threadsafe(ptb_app.process_update(update), ASYNC_LOOP)
    return Response(status=200)

# These are simple handlers for the success/cancel URLs, mainly for user feedback.
@flask_app.route("/success")
def success():
    return "Payment successful! You can now close this window and return to Telegram."

@flask_app.route("/cancel")
def cancel():
    return "Payment cancelled. You can close this window and return to Telegram."

def setup_application():
    """
    Initializes the PTB application, sets handlers, and webhook.
    Designed to be idempotent and safe to call multiple times.
    """
    global ptb_app, bot_initialized
    with bot_init_lock:
        if bot_initialized:
            return

        # Check for required environment variables
        if not all([BOT_TOKEN, GOOGLE_API_KEY, WEBHOOK_URL]):
            logger.error("One or more critical environment variables are missing. Cannot initialize bot.")
            return
        
        # Check for at least one payment provider
        stripe_configured = all([STRIPE_API_KEY, STRIPE_WEBHOOK_SECRET])
        paypal_configured = all([PAYPAL_CLIENT_ID, PAYPAL_CLIENT_SECRET, PAYPAL_WEBHOOK_ID])

        if not (stripe_configured or paypal_configured):
            logger.error("No payment provider configured. Please set Stripe or PayPal env variables.")
            return

        # On AWS Lambda, only the /tmp directory is writable. Use it for the persistence file.
        persistence_path = "/tmp/bot_data.pickle" if "AWS_LAMBDA_FUNCTION_NAME" in os.environ else "bot_data.pickle"
        persistence = PicklePersistence(filepath=persistence_path)

        # Disable connection pooling to potentially resolve intermittent network errors by
        # forcing a new connection for each request. This is more robust in some environments.
        builder = (
            Application.builder()
            .token(BOT_TOKEN)
            .job_queue(None)
            .connection_pool_size(0)
            .read_timeout(30)
            .connect_timeout(30)
            .http_version("1.1")  # Force HTTP/1.1 to improve connection stability
        )
        ptb_app = builder.persistence(persistence).build()

        # Register all handlers
        ptb_app.add_error_handler(error_handler)
        ptb_app.add_handler(CommandHandler("start", start))
        ptb_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
        ptb_app.add_handler(CallbackQueryHandler(handle_keywords_done, pattern="^keywords_done$"))
        ptb_app.add_handler(CallbackQueryHandler(handle_start_scraping_callback, pattern="^start_scraping$"))
        ptb_app.add_handler(CallbackQueryHandler(handle_stripe_pay_callback, pattern="^pay_stripe$"))
        ptb_app.add_handler(CallbackQueryHandler(handle_paypal_pay_callback, pattern="^pay_paypal$"))
        ptb_app.add_handler(CallbackQueryHandler(handle_filter_selection, pattern="^filter_"))
        ptb_app.add_handler(CallbackQueryHandler(handle_set_filter, pattern="^set_filter_"))
        ptb_app.add_handler(CallbackQueryHandler(handle_back_to_filters, pattern="^back_to_filters$"))

        # Start the asyncio event loop in a background thread if not already running
        if not async_thread.is_alive():
            async_thread.start()
            logger.info("Asyncio event loop started in a background thread.")

        # Initialize the application and set the webhook
        async def setup_bot():
            await ptb_app.initialize()
            await ptb_app.start()
            await ptb_app.bot.set_webhook(url=f"{WEBHOOK_URL}/telegram")
            logger.info("PTB Application initialized, started, and webhook set.")

        future = asyncio.run_coroutine_threadsafe(setup_bot(), ASYNC_LOOP)
        try:
            future.result(20)  # Increased
            bot_initialized = True
            logger.info("PTB Application is initialized and running.")
        except Exception as e:
            logger.error(f"Failed to initialize and start PTB Application: {e}", exc_info=True)
            return

# --- ENTRY POINT ---
if __name__ == "__main__":
    # Setup the bot application first
    setup_application()
    # For local testing, run the Flask app with Waitress
    serve(flask_app, host="0.0.0.0", port=PORT)
