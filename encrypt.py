from bcrypt import hashpw, gensalt
hashed = hashpw('postgres', gensalt())
print hashed