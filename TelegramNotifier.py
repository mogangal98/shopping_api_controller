# Will send message to admins.
import requests
class Notifier:
    def __init__(self):
        TELEGRAM_BOT_TOKEN = "x" # Telegram Bot token
        TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
        chat_ids = [0,1] # Telegram id list
        self.api_url = TELEGRAM_API_URL
        self.chat_ids = chat_ids

    def send_message(self, text: str) -> bool:
        for chat_id in self.chat_ids:
            for _ in range(5):  # 5 tries
                try:
                    response = requests.post(
                        f"{self.api_url}/sendMessage",
                        data={"chat_id": chat_id, "text": text},
                        timeout=3
                    )
                    response.raise_for_status()
                    return True
                except Exception as e:
                    print(f"Error sending Telegram message: {e}")
        return False