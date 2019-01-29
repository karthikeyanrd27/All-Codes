def zero():
    print ("You typed zero")

def sqr():
    print ("n is a perfect square")

def even():
    print ("n is an even number")

def prime():
    print ("n is a prime number")

var_str = 'astring'
if var_str == 'string':
   option = 0 

options = {0 : zero,
           1 : sqr,
           4 : sqr,
           9 : sqr,
           2 : even,
           3 : prime,
           5 : prime,
           7 : prime,
}[option]()


