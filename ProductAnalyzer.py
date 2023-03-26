from database import Database
db = Database(database="Mercado", collection="data")
db.resetDatabase()
data = db.collection.find()

def Total_cliente_B():
    return db.collection.aggregate ([
        {"$unwind": "$produtos"},
        {"$match": {"cliente_id":"B"}},
        {"$group": {"_id": {"cliente": "$cliente_id"}, "total": {"$sum": {"$multiply": ["$produtos.quantidade", "$produtos.preco"]}}}},
])
def Produto_menos_vendido():
    return db.collection.aggregate([
    {"$unwind": "$produtos"},
    {"$group": {"_id": "$produtos.nome", "total_vendido": {"$sum": "$produtos.quantidade"}}},
    {"$sort": {"total_vendido": 1}},
    {"$limit": 1}
])
def Cliente_que_menos_gastou_em_uma_unica_compra():
    return db.collection.aggregate([
        {"$unwind": "$produtos"},
        {"$group": {"_id": {"cliente": "$cliente_id"},
                    "total": {"$sum": {"$multiply": ["$produtos.quantidade", "$produtos.preco"]}}}},
        {"$sort": {"_id.data": 1, "total": 1}},
        {"$group": {"_id": "$_id.data", "cliente": {"$first": "$_id.cliente"}, "total": {"$first": "$total"}}}

])
def Lista_de_produtos_que_tiveram_uma_quantidade_vendida_acima_de_2_unidades():
    return db.collection.aggregate([
    {"$unwind": "$produtos"},
    {"$match": {"produtos.quantidade": {"$gt": 2}}},
    {"$group": {"_id": "$produtos.nome"}}
])
