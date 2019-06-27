Encrypt or Decrypt any type of field from a record.

first, you need to choose an algorithm to use, for now, EncryptField supports three symmetric algorithms: AES DES DESede, and one asymmetric algorithm: RSA.

you can choose only an algorithm or a complete transformation, a transformation is of the form: "algorithm/mode/padding"
For now EncryptField supports these transformation:


    "AES/CBC/NoPadding"
    "AES/CBC/PKCS5Padding"
    "AES/ECB/NoPadding"
    "AES/ECB/PKCS5Padding"
    "DES/CBC/NoPadding"
    "DES/CBC/PKCS5Padding"
    "DES/ECB/NoPadding"
    "DES/ECB/PKCS5Padding"
    "DESede/CBC/NoPadding"
    "DESede/CBC/PKCS5Padding"
    "DESede/ECB/NoPadding"
    "DESede/ECB/PKCS5Padding"
    "RSA/ECB/PKCS1Padding"
    "RSA/ECB/OAEPWithSHA-1AndMGF1Padding"
    "RSA/ECB/OAEPWithSHA-256AndMGF1Padding"


then you need to choose your MODE, which is "ENCRYPT_MODE" or "DECRYPT_MODE".

you can choose a charset encoding to use.

Depending on the algorithm or transformation you've chosen, different properties required::


 * if you choose AES, DES or DESede algorithm with or without mode and padding, you need to enter a key for encrypting/decrypting:
   if DES: key length must be a multiple of 8 bytes.
   if AES: key length must be a multiple of 16 bytes.
   if DESede: key length must be a multiple of 24 bytes.

 * but if you choose the RSA algorithm with or without mode and padding, you need to specify the path for the public key in case of encrypting and private key in case of decrypting.
       For now we only support keys of format DER or PEM.

     ** for DER format:
       Here is an example to generate both public and private keys --with DER-- format with these commands:
	|
	|               $ openssl genrsa -out keypair.pem 4096  (you can use 1024 or 2048 also)
	|               Generating RSA private key, 4096 bit long modulus
	|                ............+++
	|                ................................+++
	|               e is 65537 (0x10001)
	|               $ openssl rsa -in keypair.pem -outform DER -pubout -out public.der
	|               writing RSA key
	|               $ openssl pkcs8 -topk8 -nocrypt -in keypair.pem -outform DER -out private.der

       then provide the path for the public.der if encrypting, or the path for private.der if decrypting.

       -or you can use ssh-keygen but you need to add option -m PEM into your ssh-keygen command. ex:
	|
       	|		$ ssh-keygen -m PEM -t rsa -b 4096 -C "your_email@example.com"
	|
       to force ssh-keygen to export as PEM format.
       then you need Convert private Key to PKCS#8 format (so Java can read it) :
	|
       	|		$ openssl pkcs8 -topk8 -inform PEM -outform DER -in private_key.pem -out private_key.der -nocrypt
	|
       Output public key portion in DER format (so Java can read it) :
	|
       	|		$ openssl rsa -in private_key.pem -pubout -outform DER -out public_key.der
	|

     ** for PEM format:
       Here is an example to generate both public and private keys --with PEM-- format with these commands:
	|
	|		$  openssl genrsa -out keypair.pem 4096
	|		Generating RSA private key, 4096 bit long modulus (2 primes)
	|		.............................++++
	|		....................................................++++
	|		e is 65537 (0x010001)
	|
	|		$  openssl rsa -in keypair.pem -outform PEM -pubout -out public.pem
	|		writing RSA key
      	|
	|		$ openssl pkcs8 -topk8 -nocrypt -in keypair.pem -outform PEM -out private.pem
	|
	or you can just use ssh-keygen but you need to add option -m PEM into your ssh-keygen command. ex:
	|
	|		$  ssh-keygen -m PEM -t rsa -b 4096
	|		Generating public/private rsa key pair.
	|		Enter file in which to save the key (/home/ubuntu/.ssh/id_rsa):
	|		Enter passphrase (empty for no passphrase):
	|		Enter same passphrase again:
	|		Your identification has been saved in id_rsa.
	|		Your public key has been saved in id_rsa.pub.
	|		The key fingerprint is:
	|		SHA256:X1AsVUNVRnvHsrb1FXF1vqI1To+lpycPo+iaY5coJns ubuntu@ubuntu-TUF-Gaming-FX505GE-FX505GE
	|		The key's randomart image is:
	|		+---[RSA 4096]----+
	|		|           ooo++@|
	|		|          ...  ==|
	|		|          ..  .o=|
	|		|           .   o=|
	|		|        S   .=ooo|
	|		|         . .=.Boo|
	|		|        . o. *.o.|
	|		|   . E +.o. ..=. |
	|		|   .= oo=o . .+. |
	|		+----[SHA256]-----+
	|
	|		$  openssl pkcs8 -topk8 -inform PEM -outform PEM -in id_rsa -out private_key.pem -nocrypt
	|
	|		$  openssl rsa -in private_key.pem -pubout -outform PEM -out public_key.pem
	|		writing RSA key
	|

 * if you choose a ".../NoPadding" transformation you can only encrypt/decrypt a string of length a multiple of 16. (data of length a multiple of 16).

 *with any other padding, you can encrypt/decrypt any type of field of any length.

 *if you choose a ".../CBC/..." transformation you need to give an IV[] in input:
   for AES IV[] must be of length 16.
   for DES, DESede IV[] must be of length 8.
   (if IV[]'s length is wrong EncryptField will use its own IV[])

 *if you choose a ".../ECB/..." transformation you could not enter an IV[].

------

 *for encrypting you need to specify the field or fields that you want to encrypt by adding a DynamicProperty where you give the name and the type of the field. ( the type in encryption mode is optional) ex: "name_of_the_field":"type_of_the_type"
           or: "name_of_the_field":""
 	   the output is the field encrypted so in type byte array.

 *for decrypting you need to specify the field or fields that you want to encrypt by adding a DynamicProperty where you give the name and the type of the field before encrypting it (the original type of the field). ex: "name_of_the_field":"type_of_the_type"
           the output is the field decrypted so in the original type of the field before encrypting it.






