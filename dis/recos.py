import re

# input: two list(column)
def regcos(l1, l2):
    return
#     return <final_distance>

def cos_dis(l1, l2):
    return
#     return <distance_value>




def match_regx(item):
    state = '^.*?(Alabama|Alaska|Arizona|Arkansas|California|Colorado|Connecticut|Delaware|Florida|Georgia|Hawaii|Idaho|Illinois|Indiana|Iowa|Kansas|Kentucky|Louisiana|Maine|Maryland|Massachusetts|Michigan|Minnesota|Mississippi|Missouri|Montana|Nebraska|Nevada|New[ ]Hampshire|New[ ]Jersey|New[ ]Mexico|New[ ]York|North[ ]Carolina|North[ ]Dakota|Ohio|Oklahoma|Oregon|Pennsylvania|Rhode[ ]Island|South[ ]Carolina|South[ ]Dakota|Tennessee|Texas|Utah|Vermont|Virginia|Washington|West[ ]Virginia|Wisconsin|Wyoming).*?$'
    state_abbr = '^.*?(AL|AK|AS|AZ|AR|CA|CO|CT|DE|DC|FM|FL|GA|GU|HI|ID|IL|IN|IA|KS|KY|LA|ME|MH|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|MP|OH|OK|OR|PW|PA|PR|RI|SC|SD|TN|TX|UT|VT|VI|VA|WA|WV|WI|WY).*?$'
    post_code = '^.*?\b\d{5}(?:-\d{4})?\b.*?$'
    street = '^.*?\d+[ ](?:[A-Za-z0-9.-]+[ ]?)+(?:Avenue|Lane|Road|Boulevard|Drive|Street|Ave|Dr|Rd|Blvd|Ln|St)\.?.*?$'

    pattern_phone = re.compile('^\s*(?:\+?(\d{1,3}))?[-. (]*(\d{3})[-. )]*(\d{3})[-. ]*(\d{4})(?: *x(\d+))?\s*$')
    pattern_email = re.compile('^[a-z]([a-z0-9]*[-_]?[a-z0-9]+)*@([a-z0-9]*[-_]?[a-z0-9]+)+[\.][a-z]{2,3}([\.][a-z]{2})?$')
    pattern_url = re.compile('((http|https)\:\/\/)?[a-zA-Z0-9\.\/\?\:@\-_=#]+\.([a-zA-Z]){2,6}([a-zA-Z0-9\.\&\/\?\:@\-_=#])*')
    pattern_html_tag = re.compile('')
    pattern_ip = re.compile('(\d{3}\.){1,3}\d{1,3}\.\d{1,3}')
    pattern_hex = re.compile('(0)?x[0-9a-fA-F]+')
    pattern_base62 = re.compile('^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{4})$')

    pattern_state = re.compile(state)
    pattern_state_abbr = re.compile(state_abbr)
    pattern_post_code = re.compile(post_code)
    pattern_street = re.compile(street)
    

    if pattern_email.match(item) != None:
        return 1
    if pattern_url.match(item) != None:
        return 2
    if (pattern_state.match(item) != None or pattern_state_abbr.match(item) != None) or pattern_post_code.match(item) != None or pattern_street.match(item) != None:
        return 3
    if pattern_html_tag.match(item) != None:
        return 4
    if pattern_ip.match(item) != None:
        return 5
    if pattern_phone.match(item) != None:
        return 6
    if pattern_base62.match(item) != None:
        return 7
    if pattern_hex.match(item) != None:
        return 8
    return -1


def norm_vec(col):
    v = [0] * len(col)
    
    for i in col:
        index = match_regx(i)
        if index != -1:
            v[index] += 1
    return v
    
