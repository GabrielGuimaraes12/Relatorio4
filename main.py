from save_json import writeAJson
from database import Database

from ProductAnalyzer import *

db = Database(database="Mercado", collection="data")
db.resetDatabase()
data = db.collection.find()

result = Total_cliente_B()
writeAJson(result, "Total_cliente_B")

result1 = Produto_menos_vendido()
writeAJson(result1, "Produto_menos_vendido")

result2 = Cliente_que_menos_gastou_em_uma_unica_compra()
writeAJson(result2, "Cliente_que_menos_gastou_em_uma_unica_compra")

result3 = Lista_de_produtos_que_tiveram_uma_quantidade_vendida_acima_de_2_unidades()
writeAJson(result3, "Lista_de_produtos_que_tiveram_uma_quantidade_vendida_acima_de_2_unidades")


