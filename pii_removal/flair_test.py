import logging
import traceback
from presidio_helpers import analyzer_engine, analyze

logger = logging.getLogger("presidio-flair")
logger.setLevel(logging.INFO)

analyzer_params = ("flair", "flair/ner-english-large", "", "")

st_text = "My name is John Doe and my phone number is 212-555-1234. Email me at john.doe@example.com."

try:
    analyzer = analyzer_engine(*analyzer_params)
    
    st_analyze_results = analyze(
        *analyzer_params,
        text=st_text,
        entities=None,  # Use all supported entities
        language="en",
        score_threshold=0.35,
        return_decision_process=False,
        allow_list=[],
        deny_list=[]
    )
    
    if st_analyze_results:
        logger.info("Findings:")
        for res in st_analyze_results:
            print(res)
            entity_text = st_text[res.start:res.end]
            logger.info(f"Entity type: {res.entity_type}, Text: {entity_text}, Start: {res.start}, End: {res.end}, Confidence: {res.score}")
    else:
        logger.info("No findings")

except Exception as e:
    logger.error(e)
    traceback.print_exc()