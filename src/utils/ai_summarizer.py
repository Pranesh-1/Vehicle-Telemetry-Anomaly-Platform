import os

class AISummarizer:
    def __init__(self, api_key=None, provider="openai"):
        self.api_key = api_key
        self.provider = provider

    def generate_summary(self, metrics: dict, anomalies: list) -> str:
        """
        Generates a summary using a real LLM (if key provided) or sophisticated rule-based templating.
        """
        
        # 1. Construct the Context
        context = f"""
        Fleet Status Report:
        - Total Vehicles: {metrics.get('total_vehicles')}
        - Average Fleet Speed: {metrics.get('avg_speed', 0):.1f} km/h
        - Total Idle Events: {metrics.get('idle_events')}
        - Critical Anomalies Detected: {len(anomalies)}
        
        Top Anomalous Vehicles:
        {', '.join([str(a) for a in anomalies[:5]])}
        """

        # 2. If no API Key, return a sophisticated template (Fallback)
        if not self.api_key:
            return f"""
            **Automated Analysis (No API Key Provided)**:
            
            The fleet is currently tracking **{metrics.get('total_vehicles')} vehicles**. 
            Operations are mostly normal with an average speed of **{metrics.get('avg_speed', 0):.1f} km/h**.
            
            However, we have detected **{len(anomalies)} critical anomalies** that require attention. 
            Specifically, review the logs for **{', '.join([str(a) for a in anomalies[:3]])}** as they are showing irregular patterns.
            
            *Tip: Enter an OpenAI API Key in the sidebar to get a deeper, AI-generated tactical analysis.*
            """

        # 3. If API Key exists, call the LLM (Using Groq)
        try:
            from groq import Groq
            client = Groq(api_key=self.api_key)
            
            prompt = f"""
            You are a Fleet Operations Manager assistant. Analyze the following telemetry data and provide a pithy, executive summary 
            highlighting risks (fuel, safety, maintenance). Be professional and concise.
            
            Data:
            {context}
            """
            
            response = client.chat.completions.create(
                messages=[{"role": "user", "content": prompt}],
                model="llama-3.3-70b-versatile",
            )
            return response.choices[0].message.content
            
        except Exception as e:
            return f"❌ AI Generation Failed: {str(e)}"
