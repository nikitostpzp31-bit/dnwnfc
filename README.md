# Apple ID Monitor Bot

Telegram-бот для автоматического мониторинга Apple ID через Playwright.

## Возможности

- Список устройств с IMEI (`/devices`)
- Find My — локатор устройств (`/findmy`)
- Стирание устройства (`/erase`)
- Смена пароля (`/changepass`)
- Проверка почты iCloud (`/mail`)
- Настройки безопасности (`/security`)
- **Автономный мониторинг** — обнаружение новых устройств каждые 5 минут (`/monitor start`)
- **Автозащита** — при новом устройстве бот сам предлагает сменить пароль и стереть устройство (`/autoprotect on`)

## Установка

### 1. Клонировать и установить зависимости

```bash
cd apple_bot
pip install -r requirements.txt
playwright install chromium
```

### 2. Настроить .env

```bash
cp .env.example .env
```

Заполните `.env`:

```
TELEGRAM_TOKEN=ваш_токен_от_BotFather
OWNER_TELEGRAM_ID=ваш_telegram_id
FERNET_KEY=  # сгенерировать командой ниже
HEADLESS=false
MONITOR_INTERVAL=300
```

Сгенерировать ключ шифрования:
```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 3. Запустить бота

```bash
python main.py
```

### 4. Первоначальная настройка в Telegram

1. Отправьте `/setup` — введите email, пароль, два контрольных вопроса с ответами
2. Отправьте `/login` — бот войдёт в аккаунт (при `HEADLESS=false` откроется браузер)
3. Отправьте `/monitor start` — запустить автономный мониторинг

## Команды

| Команда | Описание |
|---|---|
| `/setup` | Настройка email, пароля, контрольных вопросов |
| `/login` | Войти в Apple ID |
| `/devices` | Список устройств с IMEI |
| `/findmy` | Find My — локатор |
| `/erase [имя]` | Стереть устройство |
| `/changepass` | Сменить пароль |
| `/mail` | Проверить почту iCloud |
| `/security` | Настройки безопасности |
| `/monitor start\|stop` | Запустить/остановить мониторинг |
| `/autoprotect on\|off` | Автозащита при новом устройстве |
| `/status` | Статус бота |
| `/tfa 123456` | Ввести код 2FA |
| `/cancel` | Отменить текущее действие |

## Автономный режим

После `/monitor start` бот каждые 5 минут проверяет Find My.

При обнаружении нового устройства:
1. Отправляет алерт с моделью, IMEI, местоположением
2. Предлагает кнопки: **Сменить пароль** / **Стереть устройство** / **Это моё устройство**

При включённой автозащите (`/autoprotect on`) — дополнительно автоматически получает полные детали устройства с IMEI.

## Структура проекта

```
apple_bot/
├── main.py                # Точка входа
├── bot.py                 # Telegram-бот (handlers, FSM, monitor loop)
├── apple_automation.py    # Playwright-автоматизация Apple iCloud
├── db.py                  # SQLite база данных
├── config.py              # Конфигурация из .env
├── logger.py              # Логгер (loguru)
├── utils.py               # Вспомогательные функции
├── requirements.txt
├── .env.example
├── pw_sessions/           # Сессии браузера (создаётся автоматически)
└── screenshots/           # Скриншоты ошибок (создаётся автоматически)
```

## Безопасность

- Пароль и ответы на контрольные вопросы хранятся зашифрованными (Fernet AES-128)
- Бот отвечает только владельцу (`OWNER_TELEGRAM_ID`)
- Сообщения с паролями удаляются сразу после отправки
- Все чувствительные действия требуют подтверждения

## Требования

- Python 3.11+
- Chromium (устанавливается через `playwright install chromium`)
- Для `HEADLESS=false` — дисплей (X11 или VNC на сервере)
