lib : 
	cc -Wall -c msocket.c
	ar rcs libmtp.a msocket.o
	gcc -Wall -L. user1.c -lmtp -o u1
	gcc -Wall -L. user2.c -lmtp -o u2
	gcc -Wall -L. user3.c -lmtp -o u3
	gcc -Wall -L. user4.c -lmtp -o u4
	gcc -Wall -L. initmsocket.c -lmtp -o initm

clean:
	rm -f *.o *.a
	rm -f u1 u2 initm
	rm -f received_file.txt