import os
import google.generativeai as genai

genai.configure(api_key=os.environ["GEMINI_API_KEY"])

for m in genai.list_models():
    # In google-generativeai, this is typically: supported_generation_methods
    methods = getattr(m, "supported_generation_methods", None) or []
    if "generateContent" in methods:
        print(m.name, methods)
