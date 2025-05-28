# Ideas

## Diapo 1: título y presentación

## Diapo 2: diseño. 

- Conocemos los requisitos, presentamos este diseño
- 3 componentes principales.
- Jerarquía: SPE administra varios Jobmanager (independientes).
- Cada jobmanager tiene varios task workers.

## Diapo 3: Mensajes SPE.

- SPE es el único contacto con cliente. 
- Contar llamadas (iniciar, añadir job y comenzar job)
- Administra job manager (llamadas crear jobmanager y lanzar una tarea)

## Diapo 4: Mensajes jobmanager.

- Independencia. Mensajes a SPE, resultados de trabajos. (hay algún proceso disponible para lanzar, finalización de un trabajo y de todos.)
- Lanzar un task a taskworker

## Diapo 5: Mensajes task worker.

- Avisar que terminó (con resultado)

## Diapo 6: Mensajes en diagrama.

- Explicar el orden lógico de procesos y llamadas. 
- Lo cuenta Alex

## Diapo 7: Implementation details

- 2 GenServers, 1 Task

## Diapos 8-10: Estados
- Explica Manolo. Qué hace cada elemento, qué implicación tiene etc.

## Diapo 11: Implementación task
- Hay con límite y sin