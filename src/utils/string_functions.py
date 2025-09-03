def replace_template_placeholders(template:str,replacements:dict):
    '''
    Replaces placeholders  defined as square brackets containing keywords in an
    input template string with values from an input dictionary.
    '''
    s = template
    for k,v in replacements.items():
        s = s.replace('[' + k + ']',v)
    return s
