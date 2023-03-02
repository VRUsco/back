# -*- coding: utf-8 -*-
"""
Created on Thu Jan 26 11:08:59 2023

@author: DESARROLLO VR
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
from sqlalchemy import create_engine
from sqlalchemy import text
from pandas import DataFrame
import json

#Aqui Creamos el servidor Flask
app = Flask(__name__)
CORS(app)

users = []
#Se realiza la conexion a la base de datos
engine = create_engine('postgresql://pruebas:pruebas@localhost:5434/pruebas')

#Definicion de endpoints para traer informacion de la base de datos
@app.route("/usuario", methods=["POST"])
def registro_usuario():
    json_data = request.json
    tipo_identificacion = json_data["tipo_identificacion"]
    identificacion = json_data["identificacion"]
    apellido = json_data["apellido"]
    nombre = json_data["nombre"]
    genero = json_data["genero"]
    fecha_nacimiento = json_data["fecha_nacimiento"]
    password = json_data["password"]
    perfil = json_data["rol"]
    #recepcion de datos y almacenamiento en variables
    
    sql = "insert into usuario ("
    sql += "tipo_identificacion,"
    sql += "identificacion,"
    sql += "apellido,"
    sql += "nombre,"
    sql += "genero,"
    sql += "fecha_nacimiento) values ("
    sql += str(tipo_identificacion)+","
    sql += "'" + identificacion+"',"
    sql += "'" + apellido+"',"
    sql += "'" + nombre+"',"
    sql += "'" + genero+"',"
    sql += "'" + fecha_nacimiento+"')"
    sql = text(sql)
    engine.execute(sql)
    #ejecucion de query
    
    sql_user_selected = text('select usuario.id from usuario where identificacion=\''+identificacion+'\'')
    resultSelectUser = engine.execute(sql_user_selected)
    #ejecucion de query
    
    dataframe_SelectUser = DataFrame(resultSelectUser.fetchall())
    dataframe_SelectUser.columns = resultSelectUser.keys()
    user_id = dataframe_SelectUser.to_json(orient='records')
    user_id = user_id.replace("[","")
    user_id = user_id.replace("]","")
    user_id = json.loads(user_id)
    _id = user_id["id"]
    #conversion de datos, a datos legibles por los clientes (unity - react)
    
    sqlUsuarioPerfil = "insert into usuario_perfil("
    sqlUsuarioPerfil += "usuario,perfil,login,password) values("
    sqlUsuarioPerfil += str(_id)+","
    sqlUsuarioPerfil += str(perfil)+","
    sqlUsuarioPerfil += "'"+identificacion+"',"
    sqlUsuarioPerfil += "'"+password+"')"
    sqlUsuarioPerfil = text(sqlUsuarioPerfil)
    engine.execute(sqlUsuarioPerfil)
    #ejecucion de query
    
    return {"status":"OK"}
    #devolucion del estado

@app.route('/usuario', methods=["GET"])
def listado_usuarios():
    sql = "SELECT u.id, ti.nombre as tipo_identificacion, u.identificacion, u.apellido, " 
    sql += "u.nombre, u.genero, u.fecha_nacimiento, pe.nombre AS rol "
    sql += "FROM usuario u INNER JOIN usuario_perfil up ON u.id ="
    sql += "up.usuario INNER JOIN perfil pe on up.perfil = pe.id "
    sql += "INNER JOIN tipo_identificacion ti on u.tipo_identificacion = ti.id"
    sql = text(sql)
    result = engine.execute(sql)

    # convert sqlalchemy.engine.result to pandas dataframe
    dataframe = DataFrame(result.fetchall())
    dataframe.columns = result.keys()
    data_json = dataframe.to_json(orient='records')
    data_json = json.loads(data_json)
    return {"status":"OK","usuarios":data_json}

@app.route('/usuario:<_id>', methods=["GET"])
def listado_usuario(_id):
    sql = text("select u.id, u.nombre, u.apellido, u.genero from usuario u where identificacion='"+_id+"'")
    result = engine.execute(sql)
    # convert sqlalchemy.engine.result to pandas dataframe
    dataframe = DataFrame(result.fetchall())
    dataframe.columns = result.keys()
    data_json = dataframe.to_json(orient='records')
    return jsonify(data_json)

@app.route("/prueba", methods=["POST"])
def registro_prueba():
    json_data = request.json
    fecha_hora = json_data["fecha_hora"]
    auxiliar = json_data["auxiliar"]
    usuario = json_data["usuario"]
    nivel = json_data["nivel"]
    grupo = json_data["grupo"]
    clave = json_data["clave"]
    
    sql = "insert into prueba ("
    sql += "fecha_hora,"
    sql += "auxiliar,"
    sql += "usuario,"
    sql += "nivel,"
    sql += "grupo, clave) values ("
    sql += "'"+fecha_hora+"',"
    sql += str(auxiliar)+","
    sql += str(usuario)+","
    sql += str(nivel)+","
    sql += str(grupo)+","
    sql += "'"+clave+"')"
    sql = text(sql)
    engine.execute(sql)
    return {"status":"OK"}

@app.route('/validation', methods=["POST"])
def validacion_login():
    json_data = request.json
    user = json_data["userId"]
    password = json_data["userPassword"]
    sql = "SELECT u.tipo_identificacion, u.identificacion, u.apellido,"
    sql += "u.nombre, u.genero, u.fecha_nacimiento, up.password, pe.nombre AS rol "
    sql += "FROM usuario u INNER JOIN usuario_perfil up ON u.id = up.usuario "
    sql += "INNER JOIN perfil pe on up.perfil = pe.id where up.login =\'"+user+"\'"
    sql = text(sql)
    result = engine.execute(sql)
    dataframe = DataFrame(result.fetchall())
    if dataframe.empty:
        return {"status":"FAIL","message":"Usuario no existe"}
    else:
        dataframe.columns = result.keys()
        data_json = dataframe.to_json(orient='records')
        data_json = data_json.replace("[","")
        data_json = data_json.replace("]","")
        data_json = json.loads(data_json)
        
        passBase = data_json["password"]
        
        if passBase == password:
            data_json.pop("password")
            return {"status":"OK","usuario":data_json}
        else:
            return {"status":"FAIL","message":"contraseña incorrecta"}

@app.route('/pruebas', methods=["GET"])
def listado_pruebas():
    sql = "SELECT pu.id, pu.fecha_hora, pu.auxiliar, pu.usuario," 
    sql += "n.nombre as nivel, gu.nombre as grupo from prueba pu "
    sql += "inner join nivel n on n.id = pu.nivel "
    sql += "inner join grupo gu on gu.id = pu.grupo"
    sql = text(sql)
    result = engine.execute(sql)
    # convert sqlalchemy.engine.result to pandas dataframe
    dataframe = DataFrame(result.fetchall())
    dataframe.columns = result.keys()
    data_json = dataframe.to_json(orient='records')
    data_json = json.loads(data_json)
    
    for i in data_json:
        auxiliar = i["auxiliar"]
        del i["auxiliar"]
        usuario = i["usuario"]
        del i["usuario"]
        
        sql_auxiliar = "select u.identificacion from usuario u inner join "
        sql_auxiliar += "usuario_perfil up on u.id = up.usuario where u.id ="+str(auxiliar);
        result_auxiliar = engine.execute(sql_auxiliar)
        # convert sqlalchemy.engine.result to pandas dataframe
        dataframe_auxiliar = DataFrame(result_auxiliar.fetchall())
        dataframe_auxiliar.columns = result_auxiliar.keys()
        data_json_auxiliar = dataframe_auxiliar.to_json(orient='records')
        data_json_auxiliar = data_json_auxiliar.replace("[","")
        data_json_auxiliar = data_json_auxiliar.replace("]","")
        data_json_auxiliar = json.loads(data_json_auxiliar)
        auxiliar_nombre = data_json_auxiliar["identificacion"]
        i['auxiliar'] = auxiliar_nombre
        
        sql_paciente = "select u.identificacion from usuario u inner join "
        sql_paciente += "usuario_perfil up on u.id = up.usuario where u.id ="+str(usuario);
        result_paciente = engine.execute(sql_paciente)
        # convert sqlalchemy.engine.result to pandas dataframe
        dataframe_paciente = DataFrame(result_paciente.fetchall())
        dataframe_paciente.columns = result_paciente.keys()
        data_json_paciente = dataframe_paciente.to_json(orient='records')
        data_json_paciente = data_json_paciente.replace("[","")
        data_json_paciente = data_json_paciente.replace("]","")
        data_json_paciente = json.loads(data_json_paciente)
        paciente_nombre = data_json_paciente["identificacion"]
        i['usuario'] = paciente_nombre
    return {"status":"OK", "pruebas":data_json}

@app.route('/prueba', methods=["GET"])
def listado_prueba():
    sql = "SELECT pu.id, pu.fecha_hora, pu.auxiliar, pu.paciente," 
    sql += "n.nombre as nivel, gu.nombre as grupo from prueba pu "
    sql += "inner join nivel n on n.id = pu.nivel "
    sql += "inner join grupo gu on gu.id = pu.grupo"
    sql = text(sql)
    result = engine.execute(sql)
    # convert sqlalchemy.engine.result to pandas dataframe
    dataframe = DataFrame(result.fetchall())
    dataframe.columns = result.keys()
    data_json = dataframe.to_json(orient='records')
    data_json = json.loads(data_json)
    
    print(data_json)
    
    auxiliar = data_json["auxiliar"]
    
    print(auxiliar)
    
    sql_auxiliar = text("select u.nombre from usuario u inner join usuario_perfil up on u.id = up.usuario where u.id ="+auxiliar);
    result_auxiliar = engine.execute(sql_auxiliar)
    # convert sqlalchemy.engine.result to pandas dataframe
    dataframe_auxiliar = DataFrame(result_auxiliar.fetchall())
    dataframe_auxiliar.columns = result_auxiliar.keys()
    data_json_auxiliar = dataframe_auxiliar.to_json(orient='records')
    data_json_auxiliar = json.loads(data_json_auxiliar)
    
    print(data_json_auxiliar)
    
    return {"status":"OK"}

@app.route('/auxiliares', methods=["GET"])
def listado_auxiliares():
    sql = "select u.id, u.nombre from usuario u "
    sql += "inner join usuario_perfil up on u.id = up.usuario "
    sql += "inner join perfil pe on up.perfil = pe.id "
    sql += "where pe.nombre = 'auxiliar'"
    sql = text(sql)
    
    result = engine.execute(sql)
    # convert sqlalchemy.engine.result to pandas dataframe
    dataframe = DataFrame(result.fetchall())
    dataframe.columns = result.keys()
    data_json = dataframe.to_json(orient='records')
    data_json = json.loads(data_json)
    return jsonify(data_json)

@app.route('/tipo_identificacion', methods=["GET"])
def listado_tipo_identificacion():
    sql= text("SELECT ti.id, ti.nombre from tipo_identificacion ti")
    result = engine.execute(sql)
    # convert sqlalchemy.engine.result to pandas dataframe
    dataframe = DataFrame(result.fetchall())
    dataframe.columns = result.keys()
    data_json = dataframe.to_json(orient='records')
    data_json = json.loads(data_json)
    return {"status":"OK", "tipo":data_json}

@app.route('/level', methods=["GET"])
def listado_levels():
    sql = text("select n.id, n.nombre from nivel n")
    result = engine.execute(sql)
    # convert sqlalchemy.engine.result to pandas dataframe
    dataframe = DataFrame(result.fetchall())
    dataframe.columns = result.keys()
    data_json = dataframe.to_json(orient='records')
    data_json = json.loads(data_json)
    return jsonify(data_json)

@app.route('/grupos', methods=["GET"])
def listado_grupos():
    sql = text("select g.id, g.nombre, g.descripcion from grupo g")
    result = engine.execute(sql)
    # convert sqlalchemy.engine.result to pandas dataframe
    dataframe = DataFrame(result.fetchall())
    dataframe.columns = result.keys()
    data_json = dataframe.to_json(orient='records')
    data_json = json.loads(data_json)
    return {"status":"OK","grupos":data_json}

@app.route('/gruposUnity', methods=["GET"])
def listado_grupos_unity():
    sql = text("select g.id, g.nombre, g.descripcion from grupo g")
    result = engine.execute(sql)
    # convert sqlalchemy.engine.result to pandas dataframe
    dataframe = DataFrame(result.fetchall())
    dataframe.columns = result.keys()
    data_json = dataframe.to_json(orient='records')
    data_json = json.loads(data_json)
    return jsonify(data_json)

@app.route('/grupos', methods=["POST"])
def registro_grupo():
    json_data = request.json
    nombre = json_data["nombre"]
    descripcion = json_data["descripcion"]
    sql = text("insert into grupo (nombre,descripcion) values ('"+nombre+"','"+descripcion+"')")
    engine.execute(sql)
    
    sql_groups = text("select g.id, g.nombre, g.descripcion from grupo g")
    result = engine.execute(sql_groups)
    # convert sqlalchemy.engine.result to pandas dataframe
    dataframe = DataFrame(result.fetchall())
    dataframe.columns = result.keys()
    data_json = dataframe.to_json(orient='records')
    data_json = json.loads(data_json)
    return {"status":"OK","grupos":data_json}

@app.route('/resultados', methods=["POST"])
def registro_resultados():
    json_data = request.json
    prueba = json_data["prueba"]
    nivel = json_data["nivel"]
    fecha_inicio = json_data["fecha_hora_inicio"]
    fecha_fin = json_data["fecha_hora_fin"]
    tiempo = json_data["tiempo_ejecucion"]
    sql = "insert into resultado ("
    sql += "prueba,"
    sql += "nivel,"
    sql += "fecha_hora_inicio,"
    sql += "fecha_hora_fin,"
    sql += "tiempo_ejecucion) values ("
    sql += str(prueba)+","
    sql += str(nivel)+","
    sql += "'"+fecha_inicio+"',"
    sql += "'"+fecha_fin+"',"
    sql += "'"+tiempo+"')"
    sql = text(sql)
    engine.execute(sql)
    return {"status":"OK"}

@app.route('/pruebaId:<clave>', methods=["GET"])
def id_prueba(clave):
    sql = text(" select pu.id from prueba pu where pu.clave ="+clave)
    result = engine.execute(sql)
    # convert sqlalchemy.engine.result to pandas dataframe
    dataframe = DataFrame(result.fetchall())
    dataframe.columns = result.keys()
    data_json = dataframe.to_json(orient='records')
    data_json = json.loads(data_json)
    return jsonify(data_json)

@app.route('/error', methods=["POST"])
def registro_error():
    json_data = request.json
    prueba = json_data["prueba"]
    tipo_error = json_data["tipo_error"]
    fecha_hora = json_data["fecha_hora"]
    sql = "insert into prueba_error ("
    sql += "prueba,"
    sql += "tipo_error,"
    sql += "fecha_hora) values ("
    sql += str(prueba)+","
    sql += str(tipo_error)+","
    sql += "'"+fecha_hora+"')"
    sql = text(sql)
    engine.execute(sql)
    return {"status":"OK"}

@app.route('/errores:<int:_id>', methods=["GET"])
def listado_errores(_id):
    sql = "Select pe.id, ti.descripcion as tipo_error, pe.fecha_hora "
    sql += "from prueba_error pe inner join tipo_error ti on pe.tipo_error ="
    sql += "ti.id inner join prueba p on pe.prueba = p.id where p.id ="+str(_id)
    sql = text(sql)
    result = engine.execute(sql)
    # convert sqlalchemy.engine.result to pandas dataframe
    dataframe = DataFrame(result.fetchall())
    
    if dataframe.empty:
        return {"status":"OK","message":"La prueba no contiene errores"}
    else:
        dataframe.columns = result.keys()
        data_json = dataframe.to_json(orient='records')
        data_json = json.loads(data_json)
        return {"status":"OK","Errores":data_json}


@app.route('/error:<tipo_error>', methods=["GET"])
def id_error(tipo_error):
    sql = text(" select er.id from tipo_error er where er.nombre ="+tipo_error)
    result = engine.execute(sql)
    # convert sqlalchemy.engine.result to pandas dataframe
    dataframe = DataFrame(result.fetchall())
    dataframe.columns = result.keys()
    data_json = dataframe.to_json(orient='records')
    data_json = json.loads(data_json)
    return jsonify(data_json)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)
    #app.run()