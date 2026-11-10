# Carga do Radar no Databricks

Abra este repositório em uma pasta Git do Databricks e execute `notebooks/radar_sidra.py` com acesso de escrita aos esquemas `workspace.*_economia`. Se o diretório raiz não estiver no caminho de importação da sessão, adicione a raiz da pasta Git antes de executar o notebook.

Preencha `areas_n7`, `itens_c315` e `competencias_fim` com listas de códigos separados por vírgulas. Use competências no formato AAAAMM. O notebook inclui o Brasil e consulta os meses necessários para todas as janelas pedidas.

Para corrigir uma taxa histórica, informe todas as competências finais já publicadas que incluem aquele mês. O notebook recalcula as janelas informadas. Configure o job para executar uma carga por vez.

A Bronze conserva os lotes recebidos, inclusive se o contrato falhar. Rejeições do contrato ficam em `workspace.silver_economia.sidra_rejeicoes`. Uma coleta incompleta interrompe o notebook antes dos merges. Os merges da Silver e da Gold são transações separadas. Se a Gold falhar após a gravação da Silver, reexecute o mesmo recorte.
