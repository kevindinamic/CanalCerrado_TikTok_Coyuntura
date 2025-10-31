import asyncio
from apify_client import ApifyClient
import json
import os
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv, find_dotenv
import aiohttp
from openai import OpenAI
import pandas as pd

# Clear any existing environment variables
keys = ['apify_key', 'canal_cerrado_telegram_bot_token', 'canal_cerrado_telegram_chat_id', 'openai_key']

for key in keys:
    if key in os.environ:
        del os.environ[key]

# Load dotenv()
try:
    load_dotenv(find_dotenv())
    print("Environment variables loaded successfully")
except Exception as e:
    print(f"An error occurred while loading the environment variables: {e}")

# Accesing Environment Variables
apify_key = os.getenv("apify_token")                                                   
canal_cerrado_telegram_bot_token = os.getenv('canal_cerrado_telegram_bot_token')
canal_cerrado_telegram_chat_id = os.getenv('canal_cerrado_telegram_chat_id')
openai_key = os.getenv('openai_key')

print("Environment Variables Loaded from Functions:")
print(f"APIFY_API_KEY: {apify_key}")
print(f"canal_cerrado_telegram_bot_token: {canal_cerrado_telegram_bot_token}")
print(f"canal_cerrado_telegram_chat_id: {canal_cerrado_telegram_chat_id}")
print(f"openai_key: {openai_key}")


# Cliente de Apify
client = ApifyClient(apify_key)

# Cliente de Open AI
client_openai = OpenAI(api_key=openai_key)

# Función para clasificar comentarios utilizando OpenAI
def clasificacion_texto(texto):
    completion = client_openai.chat.completions.create(
        model="gpt-4-turbo",
        messages=[
            {
                "role": "system",
                "content": (
                    "Eres un sistema que clasifica comentarios dirigidos a la Secretaría Técnica "
                    "Ecuador Crece Sin Desnutrición Infantil. Debes responder únicamente con una "
                    "de las siguientes categorías: 'Positivo' (actitud positiva, emojis de apoyo, etc), "
                    "'Negativo' (descontento o crítica, emojis de molestia), 'Neutral' (comentario neutral) y 'Sin Contexto' (emojis o texto que no permite clasificar adecuadamente) "
                  
                )
            },
            {
                "role": "user",
                "content": (
                    f"En el contexto de comentarios dirigidos a la Secretaría Técnica Ecuador Crece "
                    f"Sin Desnutrición Infantil, clasifica el siguiente comentario como 'Positivo', "
                    f"'Negativo', 'Sin Contexto'  o 'Neutral'. Responde únicamente con una de estas tres: {texto}"
                )
            }
        ]
    )
    respuesta = completion.choices[0].message.content.strip()
    return respuesta

def clasificacion_banisi(texto):
    """
    Clasifica un comentario sobre Banisi Panamá en tres dimensiones:
    - sentimiento
    - tema
    - tipo
    
    Devuelve un dict con las tres clasificaciones.
    """
    completion = client_openai.chat.completions.create(
        model="gpt-5",
        messages=[
            {
                "role": "system",
                "content": (
                    "Eres un analista de opinión pública especializado en monitoreo de reputación bancaria. "
                    "Debes clasificar cada comentario en TRES dimensiones: sentimiento, tema y tipo.\n\n"

                    "1. SENTIMIENTO (estado emocional del comentario):\n"
                    "- 'positivo': cuando hay elogio, confianza, buena experiencia, agradecimiento o satisfacción.\n"
                    "- 'negativo': cuando hay queja, frustración, crítica, rechazo, acusación o sarcasmo crítico.\n"
                    "- 'neutral': cuando es informativo, descriptivo o pregunta sin valoración clara.\n"
                    "- 'sin contexto': cuando NO se refiere al banco Banisi (ej. otro tema, otro banco, política sin relación), "
                    "o cuando son solo emojis, spam o irrelevante.\n\n"

                    "2. TEMA (el área principal del comentario, solo UNA):\n"
                    "- 'plataformas digitales': si trata sobre la app móvil, banca en línea, Yappy, fallas técnicas.\n"
                    "- 'operaciones financieras': si menciona transferencias, débitos, pagos, transacciones.\n"
                    "- 'atención al cliente': si habla de atención en agencia, WhatsApp, correo, falta de respuesta, tiempo de espera, necesidad de asesor.\n"
                    "- 'infraestructura': si se refiere a cajeros automáticos o sucursales físicas.\n"
                    "- 'confianza/seguridad': si cuestiona o apoya la seguridad de cuentas, cobros, comisiones, accesibilidad de horarios/canales.\n"
                    "- 'política/reputación': si menciona corrupción, lavado de dinero, vínculos políticos, reputación institucional.\n"
                    "- 'otro': si no encaja en ninguna categoría anterior.\n\n"

                    "3. TIPO DE INTERACCIÓN (intención del usuario):\n"
                    "- 'queja': reclamo explícito, insatisfacción, denuncia.\n"
                    "- 'duda': pregunta, solicitud de información, incertidumbre.\n"
                    "- 'sugerencia': propuesta o recomendación para mejorar.\n"
                    "- 'experiencia positiva': relato favorable o de satisfacción.\n"
                    "- 'comentario general': opinión o mención sin tono de reclamo ni elogio, ni intención de solicitar algo.\n\n"

                    "FORMATO DE RESPUESTA:\n"
                    "Devuelve SIEMPRE en JSON válido, por ejemplo:\n"
                    "{\n"
                    "  \"sentimiento\": \"negativo\",\n"
                    "  \"tema\": \"plataformas digitales\",\n"
                    "  \"tipo\": \"queja\"\n"
                    "}\n\n"

                    "EJEMPLOS:\n"
                    "Comentario: \"La app de Banisi es una porquería, nunca funciona para transferencias.\"\n"
                    "Respuesta:\n"
                    "{\n"
                    "  \"sentimiento\": \"negativo\",\n"
                    "  \"tema\": \"plataformas digitales\",\n"
                    "  \"tipo\": \"queja\"\n"
                    "}\n"
                )
            },
            {
                "role": "user",
                "content": f"Comentario:\n\"{texto}\"\n\nClasifica en JSON."
            }
        ]
    )
    
    return json.loads(completion.choices[0].message.content)

def clasificacion_texto_coyuntura_politica(texto):
    """
    Clasifica el sentimiento de un comentario sobre coyuntura nacional o social en Ecuador.
    Devuelve una sola palabra en minúsculas: 'positivo', 'negativo', 'neutral' o 'sin contexto'.
    """
    completion = client_openai.chat.completions.create(
        model="gpt-5",
        messages=[
            {
                "role": "system",
                "content": (
                    "Eres un analista de opinión pública especializado en monitorear la coyuntura nacional y social en Ecuador. "
                    "Tu tarea es clasificar comentarios según su tono general (positivo, negativo o neutral) "
                    "y descartar los que no aportan contexto.\n\n"

                    "OBJETIVO:\n"
                    "Analizar el sentimiento ciudadano frente a temas de actualidad, política, economía, seguridad, sociedad, "
                    "instituciones públicas o hechos relevantes del país.\n\n"

                    "REGLAS DE EXCLUSIÓN (contenido que NO debe analizarse):\n"
                    "• Comentarios vacíos o sin significado ('ok', 'gracias', 'jajaja', emojis, stickers, spam, enlaces, solo etiquetas).\n"
                
                    "CRITERIOS DE SENTIMIENTO (solo si es relevante):\n"
                    "• 'positivo': expresa aprobación, esperanza, optimismo, satisfacción o valoración favorable.\n"
                    "• 'negativo': expresa crítica, rechazo, indignación, desconfianza o frustración.\n"
                    "• 'neutral': informativo, descriptivo o sin valoración emocional clara.\n"
                    "• Si hay sarcasmo con crítica implícita → 'negativo'.\n"
                    "• Si mezcla opiniones, etiqueta según el tono dominante.\n\n"

                    "FORMATO DE RESPUESTA:\n"
                    "Responde SIEMPRE con una sola palabra en minúsculas: 'positivo', 'negativo', 'neutral' o 'sin contexto'.\n\n"

                    "EJEMPLOS:\n"
                    "1) 'Qué bueno que bajó la delincuencia en Quito' → positivo\n"
                    "2) 'Todo está cada vez peor en este país' → negativo\n"
                    "3) 'El transporte público está colapsado' → negativo\n"
                    "4) 'Hoy subió el precio de la gasolina' → neutral\n"
                    "5) 'Jajaja' → sin contexto\n"
                    "6) 'Gracias' → sin contexto\n"
                    "7) 'Al fin una buena noticia para la gente' → positivo\n"
                    "8) 'Increíble cómo sigue la corrupción en todos lados' → negativo\n"
                    "9) '😂😂' → sin contexto\n"
                    "10) 'El presidente anunció nuevas medidas económicas' → neutral\n"
                )
            },
            {
                "role": "user",
                "content": f"Comentario:\n\"{texto}\"\n\nClasifica con: positivo, negativo, neutral o sin contexto."
            }
        ]
    )
    return completion.choices[0].message.content.strip().lower()


# Se

# Send telegram message to Canal Cerrado
async def coyuntura_politica_send_telegram_message_async(post_comments, bot, chat_id):
    url = f"https://api.telegram.org/bot{bot}/sendMessage"

    comment = post_comments.get('text', '')
    if comment is None or not comment.strip():
        print("No se envía mensaje: comentario vacío o None")
        return
    
    sentimiento = post_comments['clasificacion']
    if sentimiento == "positivo":
        emoji = "🟢"
    elif sentimiento == "negativo":
        emoji = "🔴"
    elif sentimiento == "neutral":
        emoji = "⚪"
    else:
        emoji = ""
    
    message_text = (
        "🎵 Tiktok \n\n"
        f"📅🕒 Fecha y Hora: {post_comments['created_at']}\n"
        f"👤 Nombre: {post_comments['user']}\n\n"
        f"📝 Comment: {post_comments['text']}\n\n"
        f"🌍 Tiktok POST URL: {post_comments['url']}\n\n"
         f"Sentimiento: {sentimiento.capitalize()} {emoji}\n"
         f"🤖 Tipo de Usuario: Usuario Real\n"
    )

    data = {
        'chat_id': chat_id,
        'text': message_text,
        'parse_mode': 'HTML'
    }

    while True:  #try to send the message until the waiting time passes
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, data=data) as response:
                    if response.status == 429:  # Error 429: Too Many Requests
                        retry_after = int(response.headers.get("Retry-After", 31))  # get timeout
                        print(f"Too many requests. Retrying in {retry_after} seconds...")
                        await asyncio.sleep(retry_after)
                    elif response.status != 200:
                        response_data = await response.text()
                        print(f"Failed to send message: {response_data}")
                    else:
                        print("Message sent successfully to Canal Cerrado!")
                        break  # This ends if the message was sent correctly
        except aiohttp.ClientError as e:
            print(f"An error occurred: {e}")
            await asyncio.sleep(31)  # wait 31 seconds


async def send_telegram_message_async_canal_cerrado_banisi(tweet_data, bot, chat_id):
    url = f"https://api.telegram.org/bot{bot}/sendMessage"

    sentiment = tweet_data['clasificacion']
    if sentiment.strip().lower() in {"sin contexto", "sin contexto."}:
        print(f"Message ignored, sentiment is 'Sin Contexto'")
        return   

    if sentiment == "positivo":
        emoji = "🟢"
    elif sentiment == "negativo":
        emoji = "🔴"
    elif sentiment == "neutral":
        emoji = "⚪"
    else:
        emoji = ""

    message_text = (
        "🎵 Tiktok \n\n"
        f"📅🕒 Fecha y Hora: {tweet_data['created_at']}\n"
        f"👤 Nombre: {tweet_data['user']}\n\n"
        f"📝 Comentario: {tweet_data['text']}\n\n"
        f"🌍 Tiktok POST URL: {tweet_data['url']}\n\n"
        
        f"Sentimiento: {sentiment.capitalize()} {emoji}\n"
        f"📂 Categoría: {tweet_data['topic'].capitalize()}\n"
        f"🔢 Tipo: {tweet_data['interaction_type'].capitalize()}\n"

    )

    data = {
        'chat_id': chat_id,
        'text': message_text,
        'parse_mode': 'HTML'
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=data) as response:
            if response.status != 200:
                response_data = await response.text()
                print(f"Failed to send message: {response_data}")
            else:
                print("Message sent successfully to Canal Cerrado!")



def save_data_to_json(data, local_file_name, consolidated_file_name):
    # Save to local JSON file for each social media
    if not os.path.exists(local_file_name):
        # If the file does not exist, create a new one and write the data as a list
        with open(local_file_name, 'w') as file:
            json.dump([data], file, indent=4)
    else:
        # If the file exists, load the existing data
        with open(local_file_name, 'r+') as file:
            try:
                existing_data = json.load(file)
            except json.JSONDecodeError:
                existing_data = []

            # Append the new data to the list
            existing_data.append(data)

            # Set the file cursor to the beginning, truncate the file, and save the updated list
            file.seek(0)
            json.dump(existing_data, file, indent=4)
            file.truncate()

    print(f"Data saved to {local_file_name}")

    # Ensure the folder for the consolidated file exists
    consolidated_folder = os.path.dirname(consolidated_file_name)
    if not os.path.exists(consolidated_folder):
        os.makedirs(consolidated_folder)
        print(f"Created folder: {consolidated_folder}")

    # Save to consolidated JSON file
    if not os.path.exists(consolidated_file_name):
        # If the consolidated file does not exist, create it
        with open(consolidated_file_name, 'w') as file:
            json.dump([data], file, indent=4)
    else:
        # If the consolidated file exists, load its data
        with open(consolidated_file_name, 'r+') as file:
            try:
                existing_data = json.load(file)
            except json.JSONDecodeError:
                existing_data = []

            # Append the new data to the list
            existing_data.append(data)

            # Write the updated data back to the file
            file.seek(0)
            json.dump(existing_data, file, indent=4)
            file.truncate()

    print(f"Data also saved to {consolidated_file_name}\n")


# Function to load existing data and create a set for existing post_ids                       #-----------New function--------------#
def load_existing_data(file_path):
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r') as file:
                existing_data = json.load(file)
                post_ids = {post.get('comment_id') for post in existing_data}
                return post_ids
        except json.JSONDecodeError:
            return set()
    else:
        return set()


# Función para extraer los URLs de los posts más recientes
def extract_recent_urls(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    
    urls = []
    current_date = datetime.now()
    one_week_ago = current_date - timedelta(days=1)                       #<----------change the number of weeks to retrieve all comments
    
    for post in data:
        created_at_str = post.get("created_at")
        created_at = datetime.strptime(created_at_str, "%Y-%m-%d %H:%M:%S")
        
        if created_at >= one_week_ago:
            url = post.get("url")
            if url:
                urls.append(url)
    
    return urls

def load_existing_csv(filename):
    if os.path.exists(filename):
        df = pd.read_csv(filename)
    else:
        df = pd.DataFrame(columns=['Red social', 'Comentario', 'Clasificación', 'URL'])
    return df

# initializes a set with previously calculated values ​​or with an empty set                          #---------New-----------------#
seen_comments = load_existing_data('tiktok_comments.json')
print(f"Loaded {len(seen_comments)} existing comments")

# Define the number of seconds to wait before the next run
#seconds_for_next_run = 0 # 1h
seconds_for_next_run = 3600 # 1h

# Define the function to fetch Tiktok comments
async def fetch_tiktok_comments():
    try:
        # Determine the URLs of the most recent Instagram posts
        urls = extract_recent_urls('tiktok_posts.json')
        print(f"Extracting comments from {len(urls)} recent Tiktok posts")

        if not urls:
            print("No recent URLs found or failed to extract URLs.")
            return
        
        run_input = {
            "postURLs": urls,
            "commentsPerPost": 10,                                  #<-------------------- update the maximum number of comments ------
            "maxRepliesPerComment": 0,
        }

        # Run the Actor and wait for it to finish
        run = client.actor("BDec00yAmCm1QbMEI").call(run_input=run_input)

        # Fetch and print Actor results from the run's dataset (if there are any)
        for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            comment_id = item.get('cid')

            # Check if the post has already been seen
            if comment_id not in seen_comments and comment_id is not None:
                post_comments = {}
                # Extract the relevant data from the item
                created_at = item.get('createTimeISO')

                if created_at:
                    # Parse the timestamp as a datetime object in UTC
                    utc_time = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%S.%fZ")

                    # Define the timezone for Guayaquil, Ecuador (which is UTC-5)
                    guayaquil_tz = pytz.timezone('America/Guayaquil')

                    # Convert the UTC time to Guayaquil time
                    guayaquil_time = utc_time.replace(tzinfo=pytz.utc).astimezone(guayaquil_tz)

                    guayaquil_time_str = guayaquil_time.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    guayaquil_time_str = "Unknown time"

                # Extract the relevant data from the item
                post_url = item.get('videoWebUrl')
                comment = item.get('text')
                user = item.get('uniqueId')
                user_id = item.get('uid')
                user_profile = item.get('avatarThumbnail')
                created_at = guayaquil_time_str
                likes_count = item.get('diggCount')
                reply_count = item.get('replyCommentTotal')
                 
                if reply_count<5:
                    clasificacion = "Sin Contexto"
                else:
                    clasificacion = clasificacion_texto_coyuntura_politica(comment)


                # Add the extracted data to the post_comments dictionary
                post_comments['url'] = post_url
                post_comments['comment_id'] = comment_id
                post_comments['text'] = comment
                post_comments['user'] = user
                post_comments['user_profile'] = user_profile
                post_comments['created_at'] = created_at
                post_comments['likes_count'] = likes_count
                post_comments['user_id'] = user_id
                post_comments['reply_count'] = reply_count
                post_comments['clasificacion'] = clasificacion
                post_comments['red_social'] = 'Tiktok'


                # Add the post ID to the seen_posts set
                seen_comments.add(comment_id)

                # Print the extracted data
                print(f"Comment {[comment_id]} extracted")

                # Save the extracted data to a JSON file
                save_data_to_json(post_comments, 'tiktok_comments.json', "../aggregated_data/all_comments.json")

                # Skip sending the tweet if clasificacion is 'Sin Contexto'
                if post_comments["clasificacion"] == "Sin Contexto":
                    print("No se envía el comment 'Sin Contexto'")
                    return  # Exit the function without further processing 
                if reply_count<5:
                    print("No se envía el comment con menos de 5 respuestas")
                    return  # Exit the function without further processing

                # Send the message to Canal Cerrado
                await coyuntura_politica_send_telegram_message_async(post_comments, canal_cerrado_telegram_bot_token, canal_cerrado_telegram_chat_id)

            else:
                print(f"Comment {comment_id} already extracted sometime ago")
    except Exception as e:
        print(f"An error occurred while fetching comments: {e}")

async def main():
    while True:
        try:
            await fetch_tiktok_comments()
            print("Waiting for 20 minutes before the next execution... \n\n\n")
            await asyncio.sleep(seconds_for_next_run)  # Sleep for 20 minutes (1200 seconds)
        except Exception as e:
            print(f"An error occurred in the main loop: {e}")
            await asyncio.sleep(seconds_for_next_run)  # Wait before trying again to avoid rapid failure loop

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"An error occurred while running the main function: {e}")
