import os
import time
from typing import Dict
import requests


DEFAULT_MODEL = "gemma2-9b-it"
GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"

# System prompts for each topic
SYSTEM_PROMPTS = {
    "job_description": (
        "You are an expert HR advisor specializing in job descriptions. "
        "Create a comprehensive and SEO-optimized job description based ONLY on the provided information. "
        "Do not make assumptions or add information not provided. "
        "Structure your response with clear headings, subheadings, and bullet points. "
        "Use professional language and make it easy to read. "
        "Generate the content in HTML format with proper formatting."
    ),

    "key_responsibility": (
        "You are an expert HR advisor specializing in job responsibilities. "
        "Extract and structure the key responsibilities based ONLY on the information provided. "
        "Do not invent or assume responsibilities not mentioned. "
        "Create a comprehensive list with clear categorization using subheadings. "
        "Use SEO-friendly keywords naturally within the content. "
        "Format your response with a clear 'Key Responsibilities' heading and organized subheadings. "
        "Generate the content in HTML format with proper structure."
    ),
    
    "about_company": (
        "You are an expert company researcher and content writer with deep knowledge of major companies. "
        "Create a comprehensive 'About the Company' section using your knowledge of the company along with any provided information. "
        "If you recognize the company name, draw upon your knowledge of their industry, mission, values, culture, achievements, and market position. "
        "Structure the content with engaging headings and subheadings that showcase the company's strengths. "
        "Use professional, compelling language that would attract top talent. "
        "Include information about company culture, values, growth, innovation, and what makes them an attractive employer. "
        "Generate rich, SEO-optimized content in HTML format with proper formatting."
    ),
    
    "selection_process": (
        "You are an expert HR advisor specializing in recruitment processes with extensive knowledge of hiring practices. "
        "Create a realistic and detailed selection process based on the company, role, and industry standards. "
        "Design a multi-stage process that would be typical for this type of position at this company. "
        "Include specific stages like application review, technical assessments, interviews (technical, behavioral, cultural fit), and final selection. "
        "Make the process sound professional, thorough, and realistic for the industry and role level. "
        "Structure your response with clear headings for each stage and provide helpful details for candidates. "
        "Generate the content in HTML format with proper formatting."
    ),
    
    "qualification": (
        "You are an expert HR advisor specializing in job qualifications and requirements. "
        "Extract and organize the qualifications, skills, and requirements based ONLY on the information provided. "
        "Do not add standard qualifications or make assumptions about what might be needed. "
        "Structure the content with clear categorization such as education, experience, technical skills, and soft skills. "
        "Use professional language and SEO-friendly keywords naturally. "
        "Format your response with a clear 'Qualifications & Requirements' heading and organized subheadings. "
        "Generate the content in HTML format with proper structure."
    )
}

# Dynamic prompt construction for each topic
def construct_prompt(topic: str, job_description: str, company_name: str, job_title: str) -> str:
    """Construct topic-specific prompts with relevant information"""
    
    base_info = f"Company Name: {company_name}\nJob Title: {job_title}\n"
    print("Base Info:", base_info)
    
    if topic == "job_description":
        return f"""{base_info}
Job Description Information:
{job_description}

Task: Create a comprehensive job description that highlights the role, requirements, qualifications, and benefits based on the provided information. Focus on making it attractive to potential candidates while being accurate to the source material."""

    elif topic == "key_responsibility":
        return f"""{base_info}
Job Description Information:
{job_description}

Task: Extract and organize the key responsibilities and duties for this position. Focus on the specific tasks, objectives, and expectations mentioned in the job description. Group similar responsibilities under appropriate subheadings."""

    elif topic == "about_company":
        return f"""{base_info}
Task: Create a comprehensive and engaging 'About the Company' section. Use your knowledge of {company_name} along with any information provided in the job description. Include details about the company's industry position, mission, values, culture, achievements, growth, innovation, and what makes them an attractive employer. Make it compelling for potential candidates while being authentic to the company's actual reputation and market position."""

    elif topic == "selection_process":
        return f"""{base_info}
Job Description Information:
{job_description}

Task: Design a realistic and comprehensive selection process for this {job_title} position at {company_name}. Create a multi-stage hiring process that would be typical for this role and company size/industry. Include stages like application screening, technical assessments, multiple interview rounds (technical, behavioral, cultural fit), and final selection. Make it sound professional and realistic, with specific details about what candidates can expect at each stage. Consider the seniority level and technical requirements of the role."""

    elif topic == "qualification":
        return f"""{base_info}
Job Description Information:
{job_description}

Task: Extract and organize all qualifications, requirements, and skills mentioned in the job description. Include educational requirements, experience levels, technical skills, certifications, soft skills, and any other candidate requirements. Categorize them appropriately (e.g., Required vs Preferred, Technical vs Soft Skills, etc.). If specific qualifications are not detailed, work only with what's provided."""

    else:
        return f"""{base_info}
Job Description Information:
{job_description}

Task: Process the above information for the topic: {topic}"""


def call_groq_api(prompt: str, system_prompt: str, model: str = DEFAULT_MODEL) -> str:
    """Make a call to the GROQ API to generate content"""
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        print("⚠️ GROQ API key not found. Skipping AI enhancement.")
        return "AI enhancement not available - missing API key"
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.1,
    }
    
    try:
        response = requests.post(GROQ_API_URL, json=payload, headers=headers, timeout=60)
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"]
    except requests.RequestException as e:
        print(f"GROQ API error: {str(e)}")
        return f"Error generating content: {str(e)}"


def generate_ai_enhanced_content(job_description: str, company_name: str, job_title: str) -> Dict[str, str]:
    """Process a job description through GROQ AI to generate structured sections"""
    
    results = {}
    
    for topic, system_prompt in SYSTEM_PROMPTS.items():
        print(f"  🤖 Generating {topic}...")
        
        # Construct dynamic prompt for each topic
        dynamic_prompt = construct_prompt(topic, job_description, company_name, job_title)
        
        # Generate content using the dynamic prompt
        results[topic] = call_groq_api(dynamic_prompt, system_prompt)
        time.sleep(3)

    print("  ✅ AI enhancement complete.")
    return results