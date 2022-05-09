import rsa

publickey, privatekkey = rsa.newkeys(512)

message = "hello bros"

encMessage = rsa.encrypt(message.encode(), pub_key=publickey)

print("original message " + message)
print("encrypted message " + str(encMessage))

decmessage = rsa.decrypt(encMessage, privatekkey).decode()
print(decmessage)

signature = rsa.sign(message.encode(), privatekkey, 'SHA-256')
print(signature)
print(rsa.verify(message.encode(), signature, publickey))
print(type(publickey))
print(publickey)
print(rsa.key.PublicKey(publickey.n, publickey.e))

print(rsa.verify(message.encode(), signature, rsa.key.PublicKey(publickey.n, publickey.e)))
