# Publicar UMBRA en Render

Este repositorio incluye `render.yaml`. Conecta el repositorio Git en Render y
crea el servicio usando el blueprint; Render ejecutará el servidor Express,
servirá el juego y aceptará Socket.IO en `/socket.io` en el mismo dominio.
El chequeo de salud está en `/api/healthz`.

El servicio está configurado con una sola instancia porque las salas viven en
memoria. Si el proceso se reinicia, las salas activas se pierden. En el plan
gratuito, el servicio puede tardar en despertar después de estar inactivo.

Con los workflows del proyecto iniciados, puedes repetir la prueba local de dos
clientes con:

```sh
pnpm --filter @workspace/umbra-echo run test:multiplayer
```

Las cuentas, contraseñas, Geo y progreso individual continúan en el
almacenamiento local de cada navegador. El servidor solo recibe el nombre y el
avatar para mostrar a cada participante, además de las acciones de la partida
cooperativa; no sincroniza credenciales ni progreso individual.
