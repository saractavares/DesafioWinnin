# DesafioWinnin

O projeto foi construído seguindo as direções do repo <a>https://github.com/winnin/desafio_dataeng</a>, seguindo as melhores práticas de desenvolvimento.

O desafio contém alguns gaps que precisaram ser superados, são eles:
- Criar um cluster pelo Dtabricks Community Edition, porém o recurso de criar um cluster de uso geral que contempla uso de python e pyspark não está mais disponível no Community Edition, apenas no Free Trial. Optei por usar o recurso dentro do Azure que oferece 200 dólares de créditos para usar por 14 dias.
- Outro ponto, no exercício 1 e 2 no notebook analyze_creators, pede uma análize de ranking dos ultimos 6 meses, porém a api não contém dados para 2025, assim, para trazer análises com maior densidade, decidi usar os ultimos 24 meses nestes exercícios.

Considero que seria interessante haver mais passos a serem avaliados em versões futuras do desafio, como implementação de processo de Data Quality.

