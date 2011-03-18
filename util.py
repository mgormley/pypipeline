import os
import math

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
    return get_new_path(f, prefix=prefix, suffix=suffix,dir=dir)

def get_new_file(prefix="temp", suffix="", dir=None):
    def f(path):
        return open(path ,'w'), path
    return get_new_path(f, prefix=prefix, suffix=suffix,dir=dir)

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

def sweep_mult(middle_val, factor, num_vals):
    middle_val,factor = float(middle_val),float(factor)
    vals = []
    for i in range(num_vals):
        vals.append(middle_val * pow(1.0/factor, math.floor(num_vals * 0.5)) * pow(factor,i))
    return vals

if __name__ == "__main__":
    print sweep_mult(1, 10, 5)