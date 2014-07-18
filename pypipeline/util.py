import os
import math
import re

# ------------------- File reading ------------------------

def tail(filename, window=20):
    '''Returns the last window lines of a file.'''
    num_lines = 0
    all_chunks = ''
    for chunk in reverse_byte_generator(filename):
        num_lines = chunk.count('\n')
        all_chunks = chunk + all_chunks
        if num_lines >= window:
            break
    lines = all_chunks.splitlines(True)
    return lines[-window:]

def reverse_byte_generator(filename):  
    '''Generator for iterating over chunks of a file in reverse order.'''  
    with open(filename, 'r') as f:
        # Move to end of file. 2 indicates that we seek relative to the file's end.
        f.seek(0,2) 
        # Get number of bytes in file.
        num_bytes = f.tell() 
        bytes_to_read = 1024
        for i in reversed(range(0, num_bytes, bytes_to_read)):
            f.seek(i)
            yield f.read(bytes_to_read)            

def head(filename, window=20):
    '''Returns the first window lines of a file.'''
    lines = []
    f = open(filename, 'r')
    for i,line in enumerate(f):
        if i >= window:
            break 
        lines.append(line)
    return lines

def head_sentences(in_file, out_file, num_sentences):
    out = open(out_file, 'w')
    _head_sentences(in_file, out, num_sentences)
    out.close()

def _head_sentences(in_file, out, num_sentences):
    count = -1
    for line in open(in_file, 'r'):
        out.write(line)
        if line == '\n':
            count += 1
        if count == num_sentences:
            break

# ------------------- Scraping utilities ------------------------

def frange(bottom, top, delta):
    #return [x*delta + initial for x in range(1,(final-initial]
    r = []
    x = bottom
    while x <= top + 1e-13:
        r.append(x)
        x += delta
    return r

def to_str(x):
    if x == None:
        return ""
    else:
        return str(x)

def to_int(x):
    if x == None:
        return None
    else:
        return int(x)

def get_time(lines):
    user, system, elapsed = None, None, None
    for line in lines:
        match = re.search("(.*)user (.*)system (.*)elapsed", line)
        if match != None:
            user = match.group(1)
            system = match.group(2)
            elapsed = match.group(3)
            break
    return user, system, elapsed

def get_following_literal(lines, prefix, index=0, include_prefix=False):
    return get_following(lines, re.escape(prefix), index)

def get_following(lines, prefix, index=0, include_prefix=False):
    values = get_all_following(lines, prefix, include_prefix)
    return get_by_index(values, index)

def get_all_following_literal(lines, prefix, include_prefix=False):
    return get_all_group1(lines, re.escape(prefix), include_prefix)

def get_all_following(lines, prefix, include_prefix=False):
    if include_prefix:
        regex = "("+prefix+".*)"
    else:
        regex = prefix+"(.*)"

    return get_all_group1(lines, regex)

def get_group1(lines, regex, index=0):
    values = get_all_group1(lines, regex)
    return get_by_index(values, index)

def get_all_group1(lines, regex_str):
    regex = re.compile(regex_str)
    values = []
    for line in lines:
        match = regex.search(line)
        if match != None:
            values.append(match.group(1))
    return values

def get_match(lines, regex, index=0):
    values = get_all_matches(lines, regex)
    return get_by_index(values, index)

def get_all_matches(lines, regex_str):
    regex = re.compile(regex_str)
    match_list = []
    for line in lines:
        match = regex.search(line)
        if match != None:
            match_list.append(match)
    return match_list

def get_by_index(values, index):
    if index < len(values) and index > -len(values):
        return values[index]
    # TODO: special case...will this screw things up?
    if index == -1 and len(values) == 1:
        return values[0]
    return None

# ------------------- Paths ------------------------

def get_new_path(f, prefix="temp", suffix="", dir=None):
    if(dir != None):
        prefix = os.path.join(dir, prefix)
    prefix = os.path.abspath(prefix)
    num_digits = 3
    i = 0
    template = prefix + "_%" + str(num_digits) + "." + str(num_digits) + "d" + suffix
    for i in range(int(math.pow(num_digits,10))):
        path = template % (i)
        path = os.path.abspath(path)
        if not os.path.exists(path):
            return f(path)
    raise Exception("Could not create an new file/directory with prefix %s" % (prefix))

def get_new_directory(prefix="temp", suffix="", dir=None):
    def f(path):
        os.mkdir(path)
        return path
    return get_new_path(f, prefix=prefix, suffix=suffix, dir=dir)

def get_new_file(prefix="temp", suffix="", dir=None):
    def f(path):
        return open(path ,'w'), path
    return get_new_path(f, prefix=prefix, suffix=suffix,dir=dir)

# ------------------- General utilities ------------------------

def fancify_cmd(cmd):
    script = 'CMD="time ' + cmd + '"\n'
    script += 'echo $CMD\n'
    script += '$CMD\n'
    script += '''
EXIT=$?
if [[ $EXIT != 0 ]] ; then
    echo Error $EXIT
    exit $EXIT
fi
'''
    return script


# ------------------- Math ------------------------

def sweep_mult(middle_val, factor, num_vals):
    middle_val,factor = float(middle_val),float(factor)
    vals = []
    for i in range(num_vals):
        vals.append(middle_val * pow(1.0/factor, math.floor(num_vals * 0.5)) * pow(factor,i))
    return vals

def sweep_mult_low(low_val, factor, num_vals):
    return [x for x in sweep_mult(low_val, factor, num_vals*2) if x >= low_val]

if __name__ == "__main__":
    for x in sweep_mult(1, 2, 12):
        print x
