from flask import Flask, jsonify
from sqlalchemy import exc, create_engine

app=Flask(__name__)

def execute_query(query):
	engine = create_engine('postgresql+psycopg2://xaluser:123@192.168.0.22:5432/xaldatabase')
	connection = engine.connect()
	result = connection.execute(query).fetchall()
	connection.close()
	return result


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

if __name__ == '__main__':
	app.run(debug=True, port=8090)

