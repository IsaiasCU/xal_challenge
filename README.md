# xal_challenge
Hola, soy Isaias y esta es mi aplicación para el challenge de Xal.

**Basados en el contexto:**

*'The project consists to design the database based on the files attached, ingest the files attached into
the postgres database and build a REST API to get the records from the database.'*

**Y en la funcionalidad core:**

1. Design the E-R from the database and create the structure based on the files attached.
2. Ingest the data from the centos server to the postgres database.
3. At least the ‘read’ request must be supported for the API
4. The server where the API is going to be deployed must have access only to the postgres
database. And the centos server must have access only to the postgres database as well.

Pasaré a presentar mi solución:

### 1. Design the E-R from the database and create the structure based on the files attached.

El txt tiene la siguiente estructura:

![image](https://user-images.githubusercontent.com/115485666/194901726-c900f44f-3527-40d7-a015-55ce9169ed61.png)

Con base en ello propongo el siguiente modelo ER:

![image](https://user-images.githubusercontent.com/115485666/194994981-adb2686c-7de9-4677-8223-750eaf66d527.png)

Acerca del modelo existe bastante incertidumbre, el txt no contiene nulos, sin embargo, tampoco hay llaves primarias claras para las 3 entidades que se muestran: Companía, Departamento y Empleado. Con base en solo al información decidí que a la Companía pertenecen los campos 'company_name', 'address', 'city', 'state' y 'zip', suponiendo que el diagrama *gira entorno a la compañía* al traer departamentos y empleados. 

Por la falta de una llave como un RFC u otro identificador propio de una empresa lo más *lógico* para colocar como llave primaria es el nombre de la empresa y su dirección. La hipótesis es que la dirección por sí sola debe ser casi única, a excepción de los números interiores, y combinado con el nombre de la empresa la llave tiene mayor fiabilidad.

En el caso del departamento, lo consideré una entidad débil que no puede ser identificada por sí sola, por tal motivo arrastra llaves foráneas de Compañía y suma su atributo nombre a la llave primaria. Desde mi forma de pensar un Departamento debe pertenecer solo a una empresa, aún cuando el mismo concepto base del departamento se encuentre en otras compañías (como Marketing, Sales, etc.). La única forma para que un mismo departamento *trabaje* para distintas empresas, sería un tipo de outsourcing, lo cual descarté en este modelo.

Para la entidad Empleado contemplé los campos 'first_name', 'last_name', 'phone1', 'phone2' y 'email'. Como llave primaria seleccioné el email. ¿Por qué? Porque el nombre se puede repetir comunmente, lo mismo para el apellido, los números de teléfono se pueden reciclar al no usarse en un tiempo, pero el email es persistente y, en teoría, personal. Puede ser que una persona tenga más de un correo en la realidad, pero tendríamos que hacer una representación para la relación de empleado-correo que no exploré en este modelo.

Pude haber anexado IDs extras a todas las entidades, pero eso no solucionaría los problemas aquí presentados porque, al final y al cabo, el que asignaría los IDs soy yo y volvería a caer en todas las conjeturas planteadas anteriormente.

### 2. Ingest the data from the centos server to the postgres database.

Para el proceso de ingesta utilicé el docker-compose que venía incluído, sin embargo, al tratarse de un archivo TXT el que tenía que ser ingestado opté por cambiar a un servidor FTP que encontré (https://github.com/panubo/docker-vsftpd). Mi docker-compose se encuentra en este repositorio. Entonces, terminé con dos contenedores, uno con el servidor FTP y otro con PostgreSQL. Ambos con su usuario XAL y contraseña, nada muy complejo en esta prueba.

Ejecuté una Jupyter Notebook en un tercer contenedor de Python que se encargó de hacer el request al servidor FTP, guardar el archivo en CSV, hacer el procesado e insertar en la base de datos. 

El request se ve de la siguiente manera: 

```
# Datos del usuario del servidor:
HOST_NAME = "172.17.0.1"
USER_NAME = "xal"
PASSWORD = "xal"

ftp_server = ftplib.FTP(HOST_NAME, USER_NAME, PASSWORD)
filename = "sample.csv"

# Escribir en modo binario:
try:
    with open(filename, "wb") as file:
        # Intentar descargar del servidor:
        ftp_server.retrbinary(f"RETR {filename}", file.write)
    ftp_server.quit()
except Exception as e:
    # Imprimir en caso de error:
    print("Error: ", str(e))
    logging.error(traceback.format_exc())
```

El procesado de la información se realiza de la siguiente manera: 

* Se carga el archivo de datos en un Dataframe de Pandas.
* Se carga un archivo schema que contiene el nombre de las columnas, el tipo de las columnas y si puede ser nulleable o no: ![image](https://user-images.githubusercontent.com/115485666/195002976-1cd00064-e5b4-44ae-a666-12e135021445.png)
* Se comprueba que la cantidad de columnas del archivo sea igual al número de columnas del schema.
* Se comprueba que los tipos de datos de las columnas sea del mismo tipo indicado en el esquema.
* Se comprueba que el nombre de las columnas del header sea igual.

Una vez pasadas todas las pruebas sin error se inserta en la base de datos row por row del dataframe. ¿Por qué row por row? Para lidiar con las excepciones sin parar por completo el resto de inserciones. Se inserta en cada una de las tablas basados en el modelo que vimos previamente:

```
# Insert query base para ejecutar sustituyendo el nombre de la tabla y los valores:
insert_sql = """INSERT INTO ${TABLE_NAME} VALUES(${VALUES_STRING});"""
#Para cada una de las rows en el Dataframe:
for row in df[company_elements].iterrows():
    try:
        # Row es una tupla que contiene el index y la Serie con la información del row.
        # .values obtiene los valores de una Serie, que resulta un npArray.
        # list convierte el npArray a una lista común.
        # la función lambda obtiene los valores en cadenas de texto en formato
        #            '{valor}´,
        # y los concatena.
        # El [:-1] quita la última coma de la cadena.
        values = ''.join(f"'{str(x)}'," for x in list(row[1].values))[:-1]
        actual_insert_sql = insert_sql.replace('${TABLE_NAME}', 'company').replace('${VALUES_STRING}', values)
        print(actual_insert_sql)
        connection.execute(actual_insert_sql)
    except exc.IntegrityError as e:
        print(e,'\n\n')
```

### 3. At least the ‘read’ request must be supported for the API

Se implementó un método GET para obtener la información de una empresa por medio de una aplicación en Flask sumamente sencilla, por el momento es todo lo que se puede obtener con dicho GET. La API se conecta a la base de datos por medio de las credenciales del usuario XAL al cuál se le dieron todos los privigios sobre el schema public de Postgres.

```
#Función para obtener registro de compañía:
@app.route('/return_company/<string:row_id>', methods=['GET'])
def return_company_info(row_id):
	print('Entró en el caso con valor', row_id)
	try:
		query = f"select * from company where company_name = {row_id};"
		response = execute_query(query)[0]
		response_dict = {
			"company_name" : response[0],
			"address" : response[1],
			"city" : response[2],	
			"state" : response[3],	
			"zip" : response[4]
		}
	except Exception as e:
		return jsonify({'Error' : 'Error en al conexión a la db.'})
	finally:

		print(response_dict)
		return jsonify(response_dict)
```

La respuesta es similar a esta:

```
{
    "address": "6649 N Blue Gum St",
    "city": "New Orleans",
    "company_name": "Benton, John B Jr",
    "state": "LA",
    "zip": 70116
}
```

#Cambiar a SFTP.
#El schema solo reconoce enteros y cadenas.
#Falta validar los campos nulos.

