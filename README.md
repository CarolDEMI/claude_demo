# Claude Demo Web Application

A simple Flask web application demonstrating basic web development concepts.

## Features

- Home page with interactive UI
- REST API endpoints
- JSON data handling
- Responsive design

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Run the application:
   ```bash
   python app.py
   ```

3. Open your browser and navigate to `http://localhost:5000`

## API Endpoints

- `GET /` - Home page
- `GET /about` - About page
- `GET /api/hello` - Returns a JSON greeting
- `POST /api/echo` - Echoes back the JSON data sent in the request

## Project Structure

```
claude_demo/
├── app.py              # Main Flask application
├── requirements.txt    # Python dependencies
├── templates/          # HTML templates
├── static/            # Static files (CSS, JS, images)
└── src/               # Source code modules
```