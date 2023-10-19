import xmlrpc.client
import sys
import mysql.connector
import pandas as pd
import time

#Connection
url = "https://edu-smarters-sk81.odoo.com"
db = 'edu-smarters-sk81'
username = 'enzodlz@al.insper.edu.br'
password = 'Smartsk8'

#verify if the connection information is correct
common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
uid = common.authenticate(db, username, password, {})
print("User ID = ", uid)

if uid == False:
    print("erro na autenticacao de usuario")
    sys.exit()

models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))





# Define funções de operação do projeto:

def WO_Start(WO_id):
    model_name = 'mrp.workorder'
    WO_start = models.execute_kw(db,uid,password, model_name,'button_start',[WO_id])
    return WO_start


def WO_WriteProduction(WO_id, qty_produced):
    model_name = 'mrp.workorder'
    wo_write = models.execute_kw(db, uid, password, model_name, 'write', [[WO_id], {'qty_produced': qty_produced}] )
    wo_write = models.execute_kw(db, uid, password, model_name, 'write', [[WO_id], {'qty_producing': qty_produced}] )
    return wo_write


def WO_Done(WO_id):
    model_name = 'mrp.workorder'
    WO_done = models.execute_kw(db,uid,password, model_name,'button_done',[WO_id])
    return WO_done


def MO_MarkAsDone(MO_id):
    model_name = 'mrp.production'
    MO_done = models.execute_kw(db, uid, password, model_name, 'button_mark_done', [[MO_id]] )
    return MO_done  

def Search_MO():
    model_name = 'mrp.production'
    domain = [[ ['state', '=', 'confirmed'] ]]
    parameters = {'fields': ['name', 'product_id', 'product_qty', 'state', 'components_availability', 'workorder_ids']}
    records = models.execute_kw(db, uid, password, model_name, 'search_read', domain, parameters)
    return records

def Search_WO(MO_id):
    model_name = 'mrp.workorder'
    domain = [MO_id]
    parameters = {'fields':
    ['name','workcenter_id','qty_production','qty_producing','qty_produced','working_state','production_state','state','is_produced']}
    records = models.execute_kw(db, uid, password, model_name, 'read', domain, parameters)
    return records

def acha_prefixo(nome):
    fim = nome.find(']',1,10)
    prefixo = str(nome[1:fim])
    return prefixo


def checa_WO_finalizdo_odoo(id):
    model_name = 'mrp.workorder'
    domain = [id]
    parameters = {'fields':['state']}
    estado = models.execute_kw(db, uid, password, model_name, 'read', domain, parameters)[0]['state']
    return True if estado == 'progress' else False

def checa_MO_finalizdo_odoo(id):
    model_name = 'mrp.production'
    domain = [id]
    parameters = {'fields':['state']}
    estado = models.execute_kw(db, uid, password, model_name, 'read', domain, parameters)[0]['state']
    return True if estado == 'to_close' else False

def acha_prefixo_mat_prima(product_id):
    model_name = 'product.product'
    domain =  [product_id]
    parameters = {'fields':
    ['display_name']}
    records_2 = models.execute_kw(db, uid, password, model_name, 'read', domain)

    codigo_produto = records_2[0]['code']
    if type(records_2[0]['bom_line_ids']) is list:
        if len(records_2[0]['bom_line_ids']) > 0:
            acha_id = records_2[0]['bom_line_ids'][0]

            model_name = 'mrp.bom.line'
            domain =  [acha_id]
            records_3 = models.execute_kw(db, uid, password, model_name, 'read', domain)
            id_dos_filhos = records_3[0]['child_line_ids']

            if type(id_dos_filhos) == list:

                for filho in id_dos_filhos:

                    model_name = 'mrp.bom.line'
                    domain =  [filho]
                    records_4 = models.execute_kw(db, uid, password, model_name, 'read', domain)
                    sufixo = acha_prefixo(records_4[0]['display_name'])[-1]

                    if sufixo == codigo_produto[-1]:
                        return codigo_produto, acha_prefixo(records_4[0]['display_name'])


            else:

                id_do_filho = records_3[0]['child_line_ids'][0]
                model_name = 'mrp.bom.line'
                domain =  [id_do_filho]
                records_4 = models.execute_kw(db, uid, password, model_name, 'read', domain)
                return codigo_produto, acha_prefixo(records_4[0]['display_name'])
        else:
            return codigo_produto,"Nada"

def pega_dados_WO(MO_id,iterador):

    Dic_codmaq = {1:'TR10',2:'CU10',3:'MT10',4:'ET10'}

    WorkOrder = Search_WO(MO_id)
    WO_id = WorkOrder[iterador]['id']
    Cod_maq = Dic_codmaq[WorkOrder[iterador]['workcenter_id'][0]]
    Qtd_prod = WorkOrder[iterador]['qty_production']
    Nome_prod = acha_prefixo_mat_prima(Dic_MO[MO_id])[0]
    Nome_mp = acha_prefixo_mat_prima(Dic_MO[MO_id])[1]

    sql = '''INSERT INTO wo_to_factory (WO_id, MO_id, Cod_Maq, Date_ToDo, Qtd_Prod, Nome_Prod, Nome_MP, NC_Prog, WO_Status)
        VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s)'''
    val = (WO_id, MO_id, Cod_maq, "CURDATE()", Qtd_prod, Nome_prod, Nome_mp, 'O1000', 0)

    return WO_id, sql, val

# Início do ciclo Odoo SQL:
while(True):

    try:
        #Abrir a conexao com o BD
        mydb = mysql.connector.connect(
        host="smarters-db.c50q6rz9ggrg.us-east-1.rds.amazonaws.com",
        user="sk8", password="sk8", database="smarters-db-sk8" )
        #Executar consulta SQL a partir do cursor
        mycursor = mydb.cursor(buffered=True)

        #Checar MO
        Manu_Orders = Search_MO()
        if(type(Manu_Orders)==list):
            Dic_MO = {}
            for order in Manu_Orders:
                Dic_MO[order['id']] = order['product_id'][0]
        else:
            Dic_MO = {Manu_Orders['id']:Manu_Orders['product_id'][0]}
        
        i = 0
        for MO_id in Dic_MO.keys():
            #Buscar WO
            Lista_dados_WO = pega_dados_WO(MO_id,i)
            #Iniciar WO no Odoo e a fábrica
            WO_Start(Lista_dados_WO[0])
            mycursor.execute(Lista_dados_WO[1], Lista_dados_WO[2])
            i+=1


        mydb.commit()

        print("SQL executado")


        #Atualizar quantidade produzida
        sql2 = "SELECT WO_id FROM wo_to_factory WHERE WO_Status = 1"
        mycursor.execute(sql2)
        WO_ids_producing =  mycursor.fetchall()
        resultados1 = mycursor.fetchall()
        df1 = pd.DataFrame(resultados1, columns=mycursor.column_names)
        WO_ids_producing = df1['WO_id'].tolist()

        if WO_ids_producing != None:
            for id in WO_ids_producing:
                sql3 = f'''
                SELECT COUNT wo_to_factory_idWO 
                FROM pecas
                WHERE (StatusPeca = 1 AND wo_to_factory_idWO = {id})
                '''

                num_pecas = mycursor.execute(sql3)

                WO_WriteProduction(id, num_pecas)

        #Encerrar work order e mo
        sql4 = "SELECT WO_id, MO_id FROM wo_to_factory WHERE WO_Status = 3"
        mycursor.execute(sql4)
        resultados2 = mycursor.fetchall()
        df2 = pd.DataFrame(resultados2, columns=mycursor.column_names)
        # pd.set_option('max_columns', None, 'display.expand_frame_repr', False)
        WO_ids_finalizados_fabrica = df2['WO_id'].tolist()
        MO_ids_finalizados_fabrica = df2['MO_id'].tolist()
        MO_ids_finalizados_fabrica = [int(i) for i in MO_ids_finalizados_fabrica]
        
        if WO_ids_finalizados_fabrica != None:
            for WO_id in WO_ids_finalizados_fabrica:
                if checa_WO_finalizdo_odoo(WO_id):
                    print(f'Finalizando WO_id {WO_id} no odoo')
                    try:
                        WO_Done(WO_id)
                        print(f'WO com ID {WO_id} foi finalizado no Odoo')
                    except:
                        print(f'Erro finalizando WO_id {WO_id} no Odoo')
                    
            for MO_id in MO_ids_finalizados_fabrica:
                    if checa_MO_finalizdo_odoo(MO_id):
                        print(f'Finalizando MO com ID {MO_id} no Odoo')
                        try:
                            MO_MarkAsDone(MO_id)
                            print(f'MO com ID {MO_id} foi finalizado no Odoo')
                        except xmlrpc.client.Fault as err:
                            print(f'Erro finalizando MO_id {MO_id} no Odoo')
                            print(f'{err}')
                





    except mysql.connector.Error as error:
        print("Failed to run SQL {}".format(error))
        sys.exit()

    except NameError as error2:
        print("Não há itens na lista de pedidos")
        sys.exit()

    finally:
        if mydb.is_connected():
            mycursor.close()
            mydb.close()
            print("MySQL connection is closed")

    time.sleep(5)
        