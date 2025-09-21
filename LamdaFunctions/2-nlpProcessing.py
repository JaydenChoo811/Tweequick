import json
import boto3
from datetime import datetime

# Malaysian states for location extraction
MALAYSIAN_STATES = [
    "Johor", "Kedah", "Kelantan", "Melaka", "Negeri Sembilan",
    "Pahang", "Perak", "Perlis", "Penang", "Pulau Pinang",
    "Sabah", "Sarawak", "Selangor", "Terengganu", "Kuala Lumpur",
    "Labuan", "Putrajaya"
]

def invoke_bedrock_for_flood_analysis(text):
    """
    Call Bedrock to analyze if tweet is about actual flooding
    Returns: (is_flood: bool, confidence_score: float, extracted_states: list)
    """
    if not text or not text.strip():
        raise ValueError("Empty text provided for analysis")
    
    # Initialize Bedrock client
    session = boto3.Session(region_name="us-east-1")
    client = session.client("bedrock-agent-runtime")
    
    # Your agent configuration
    agent_id = "ZRK4HQFP2E"
    alias_id = "ICXLWRURH4"
    
    # Call Bedrock Agent with just the tweet text
    # System instructions are configured in the agent itself
    response = client.invoke_agent(
        agentId=agent_id,
        agentAliasId=alias_id,
        sessionId=f"nlp-{datetime.now().strftime('%Y%m%d%H%M%S%f')}",
        inputText=text,
        enableTrace=False,
        streamingConfigurations={
            "applyGuardrailInterval": 20,
            "streamFinalResponse": False
        }
    )

    print(response)
    
    completion = ""
    for event in response.get("completion", []):
        if "chunk" in event:
            chunk = event["chunk"]
            completion += chunk["bytes"].decode()

        if "trace" in event:
            trace_event = event.get("trace")
            trace = trace_event["trace"]
            for key, value in trace.items():
                print(f"{key}: {value}")

    # Parse JSON response
    import re
    json_match = re.search(r'\{.*\}', completion, re.DOTALL)
    if not json_match:
        raise ValueError("No JSON response found from Bedrock agent")
    
    json_str = json_match.group()
    result = json.loads(json_str)

    print(f"Bedrock analysis result: {json.dumps(result)}")
    
    return {
        "is_flood": result.get("is_flood", False),
        "urgency_score": result.get("urgency_score", 0),
        "confidence": result.get("confidence", 0.0),
        "extracted_locations": {
            "states": result.get("states", []),
            "cities": result.get("cities", [])
        }
    }

def lambda_handler(event, context):
    """
    Lambda 2: NLP Processing with Bedrock Integration
    
    Input: Output from Lambda 1 (with statusCode and body structure)
    Output: NLP analysis results from Bedrock
    """
    
    try:
        print(f"Received event: {json.dumps(event)}")
        
        tweet_data = None
        
        if isinstance(event, dict):
            # Check if this is Lambda 1 output format
            if "body" in event and isinstance(event["body"], str):
                # Parse the JSON body from Lambda 1
                body_data = json.loads(event["body"])
                tweet_data = body_data.get("tweet_data")
            elif "tweet_data" in event:
                # Direct tweet_data in event
                tweet_data = event["tweet_data"]
            elif "text" in event and "tweet_id" in event:
                # Event is the tweet data itself
                tweet_data = event
        
        if not tweet_data:
            raise ValueError("No tweet data found in event")
        
        # Extract tweet text and ID
        tweet_text = tweet_data.get("text", "")
        tweet_id = tweet_data.get("tweet_id", "unknown")
        
        if not tweet_text:
            raise ValueError("No tweet text provided for analysis")
        
        print(f"Analyzing tweet {tweet_id}: {tweet_text[:100]}...")
        
        # Call Bedrock for flood analysis
        bedrock_result = invoke_bedrock_for_flood_analysis(tweet_text)
        
        # Prepare analysis results
        analysis_results = {
            "tweet_id": tweet_id,
            "flood_detected": bedrock_result["is_flood"],
            "urgency_score": bedrock_result["urgency_score"],
            "extracted_locations": bedrock_result["extracted_locations"],
            "confidence": bedrock_result["confidence"],
            "processed_at": datetime.now().isoformat(),
            "analysis_method": "bedrock"
        }
        
        response_data = {
            "status": "success",
            "message": "Bedrock NLP analysis completed",
            "original_tweet": {
                "id": tweet_id,
                "text": tweet_text,
                "timestamp": tweet_data.get("timestamp"),
                "location_hint": tweet_data.get("location_hint"),
                "hashtags": tweet_data.get("hashtags", [])
            },
            "analysis": analysis_results,
        }
        
        return {
            "statusCode": 200,
            "body": json.dumps(response_data)
        }
        
    except Exception as e:
        print(f"NLP processing error: {str(e)}")
        error_response = {
            "status": "error",
            "message": f"NLP processing failed: {str(e)}",
            "tweet_id": "unknown",
            "error_details": str(e)
        }
        
        return {
            "statusCode": 500,
            "body": json.dumps(error_response)
        }