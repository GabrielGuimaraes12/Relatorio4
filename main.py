from database import Database
from save_json import writeAJson

db = Database(database="Mercado", collection="data")
db.resetDatabase()

result = db.collection.aggregate([
    {"$unwind": "$produtos"},
    {"$group": {"_id": "$cliente_id", "total": {"$sum": {"$multiply": ["$produtos.quantidade", "$produtos.preco"]}}}},
    {"$group": {"_id": None, "media": {"$avg": "$total"}}}
])

result2 = db.collection.aggregate([
    {"$unwind": "$produtos"},
    {"$group": {"_id": "$produtos.nome", "total": {"$sum": "$produtos.quantidade"}}},
    {"$sort": {"total": -1}},
    {"$limit": 1}
])

result3= db.collection.aggregate([
    {"$unwind": "$produtos"},
    {"$group": {"_id": {"cliente": "$cliente_id", "data": "$data_compra"}, "total": {"$sum": {"$multiply": ["$produtos.quantidade", "$produtos.preco"]}}}},
    {"$sort": {"_id.data": 1, "total": -1}},
    {"$group": {"_id": "$_id.data", "cliente": {"$first": "$_id.cliente"}, "total": {"$first": "$total"}}}
])
result4 = db.collection.aggregate ([
    {"$unwind": "$produtos"},
    {"$match": {"cliente": "B"}},
    {"$group": {"_id": "$cliente", "total_gasto": {"$sum": "$produtos.preco"}}}
])
result5 = db.collection.aggregate([
    {"$unwind": "$produtos"},
    {"$group": {"_id": "$produtos.nome", "total_vendido": {"$sum": "$produtos.quantidade"}}},
    {"$sort": {"total_vendido": 1}},
    {"$limit": 1}
])

result6 = db.collection.aggregate([

    {"$unwind": "$produtos"},
    {"$group": {"_id": "$cliente", "total_gasto": {"$sum": "$produtos.valor_total"}}},
    {"$sort": {"total_gasto": 1}},
    {"$limit": 1}



])
result7 = db.collection.aggregate([
    {"$unwind": "$produtos"},
    {"$match": {"produtos.quantidade": {"$gt": 2}}},
    {"$group": {"_id": "$produtos.nome"}}
])
#writeAJson(result, "Média de gasto por cliente")
#writeAJson(result2, "Produto mais vendido")
#writeAJson(result3, "Cliente que mais comprou em cada dia")
writeAJson(result4, "Total cliente B")
writeAJson(result5, "Produto menos vendido")
writeAJson(result6, "Cliente que menos gastou em uma unica compra")
writeAJson(result7, "Lista de produtos que tiveram uma quantidade vendida acima de 2 unidades")