# Sistemas Distribuidos Tarea 5 (Chord)

## Rodrigo Daniel Pino Trueba C-412

#### Explicación

Este trabajo representa una implementación en python de un sistema distribuido descentralizado en que se rige por las pautas del funcionamiento de chord.

Para su desarrollo fueron utilizados sockets ZMQ y sockets TCP.  

#### Implementación

Un nodo chord es capaz de recibir y enviar request concurrentemente debido al uso de hilos. Se utilizan sockets ZMQ del tipo router para manejar estas tareas. Existe un 3er hilo corriendo en paralelo donde se encuentra un socket TCP esperando alguna conexión. Si detecta un nodo Chord en la red con mismo tamaño de identificador le hace un pedido para establecer una conexión.

Cada determinado periodo de tiempo(1 seg.) los nodos se estabilizan. Si en este proceso un nodo no logra comunicarse con alguno de sus otros nodos conocidos. Se queda _iddle_ y es necesario que el usuario haga join manualmente. Es posible y fácil hacer que en el caso de que el nodo se quede solo en la red se mantenga en estado de busqueda persistente de cualquier nuevo que se encuentre en la red, esto no obstante puede no ser ideal en ciertos caso por tanto se deja fuera.

Durante el proceso de estabilización si el predecesor del nodo no manda un update en un intervalo de tiempo especificado, se asumira que el predecesor fallo y se aceptara a un nuevo predecesor que tenga un identificador "anterior"  al antiguo.

#### Ejecución

Para ejecutar el programa, ejecutar la siguiente línea en la consola:

> python run <arg>

<arg> representa el tamaño maximo del "círculo" de los identificadores. Dos nodos con tamaños distintos no se conectan entre ellos. 

Tambien es posible obtener una imagen del proyecto haciendo:

> docker build -t <ImageName> .

Luego para ejecutarla:

> docker run -it <ImageName> <arg>

#### Mini Cliente de Chord

Se proporciona junto al proyecto un mini-cliente para realizarles pedidos a un nodo chord.

Los pedidos del cliente de Chord se realizan a traves de los siguiented metodos:

**Comandos principales**:

* "_join_ [ip]" para unirse a una red. El parametro ip señala el lugar donde se encuentra el nodo al que se quiere unir. Este parámetro es opcional. En caso de no brindarse, el nodo hace una búsqueda automática por la red y se conecta al primer nodo que tenga la misma cantidad de bits destinadas a sus identificadores.
* "_lookup_ key" para ubicar el nodo responsable de la llave key.
* "_exit_" para que el nodo abondone la red y le avise a sus nodos allegados.

**Comandos secundarios**: Se tienen ademas una serie de comandos para obtener el estado interno de un nodo Chord:

* "ns" devuelve una lista con todos  los nodos conocidos.
* "ft" devuelve la finger table del nodo.
* "ip" devuelve los nodos a los que esta conectado el router encargado de enviar los requests.