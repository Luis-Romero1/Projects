#!/usr/bin/env python
# coding: utf-8

# In[15]:


from hdfs import InsecureClient
import json


# In[16]:


def conexion(url,usuario):
    """
    url --string no null, url del host
    
    Esta función logra la conexion con hdfs 
    
    """
    try:
        client = InsecureClient(url,usuario)
        return client
    except:
        print("Ocurrio un error favor de verificar la url del host")


def crear_directorio(client,pathhdfs):
    """
    pathhdfs --string not null 
    Esta funcion crea un directorio en hdfs, pasandole la ruta donde sera creado
    """
    try:
        pathcreado=client.makedirs(pathhdfs)
        print("se creo el directorio " + pathhdfs)
        return pathcreado
    except:
        print("Ocurrio un error favor de verificar")


# In[17]:


def cargar_archivo(client,pathhdfs,local_path):
    try:
        client.upload(pathhdfs,local_path)
        print("Se cargo el archivo" + pathhdfs)
    except:
        print("Ocurrio un error al cargar tu archivo")
    pass    


# In[ ]:


def lista_directorio(client,path):
    try:
        z=client.list(path)
        print("Se listo el archivo" + path)
        return z
    except:
        print("Ocurrio un error al listar directorio")
    pass


# In[ ]:


def lectura_HDFS(cliente,path_archivo):
    try: 
        with cliente.read("path_archivo") as reader:
            content = reader.read()
        print("Se hizo lectura de" + path_archivo)
        return content
    except:
        print("No se encontro el archivo que esta buscando")
    pass


# In[ ]:


def eliminar_directorio(client,path_hdfs):
    try:
        client.delete(path_hdfs)
        print("Se elimino el archivo en" + path_hdfs)
    except:
        print("No se pudo eliminar su archivo")
    pass


# In[ ]:


def main():
    # Lo que quiero hacer es crear una carpeta donde pondre mi archivo .json(data.json), para posteriormente listar 
    # los elementos de esa carpeta y luego eliminar el archivo
    file='data.json'
    url='http://localhost:50070'
    user="hdfs"
    client=conexion(url,user)
    crear_directorio(client,"/pruebas")
    cargar_archivo(client,"/pruebas",file)
    lista_directorio("/pruebas")
    lectura_HDFS(client,"/pruebas/data.json")
    eliminar_directorio(client,"/pruebas/data.json")
    


# In[ ]:


if __name__=='__main__':
    main()

    

