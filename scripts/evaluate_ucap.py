import sys
import yaml
from pathlib import Path

sys.path = [str(Path().cwd())] + sys.path
from evaluate_outage_rates import evaluate_outage_rates
from normalize_derations import normalize_derations
from src.ucap_evaluator.ucap_evaluator import UCAPEvaluator
from src.utils.string_functions import replace_template_placeholders

def evaluate_ucap():
    with open('config/config.yaml') as f:
        config = yaml.safe_load(f)

    ucap_evaluator = UCAPEvaluator(config)

    # evaluate_outage_rates()
    # normalize_derations()

    ucap_evaluator.evaluate_ucap()

if __name__=='__main__':
    evaluate_ucap()