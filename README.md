# cosmic-rad-data

Archivo público de observaciones de protones solares (GOES) usado por [cosmic-rad](https://github.com/ibpilot/cosmic-rad). Tiene dos capas:

- **`solar/` — operativo SWPC**, cadencia de 5 min, recolectado automáticamente cada 6 h. `solar/YYYY/MM/YYYY-MM-DD.json` (integral) y `-diff.json` (13 canales diferenciales), un fichero por día UTC completo; `manifest.json` publica último éxito, cobertura, días incompletos, satélites y versión. SWPC solo sirve 7 días: los huecos se recuperan dentro de esa ventana y después son permanentes.
- **`ncei/` — histórico oficial NCEI** (`sgps-l2-avg5m`), separado por satélite (G18/G19), un fichero por día y satélite con `ncei/manifest.json`. Se mantiene con catch-up automático, idempotente y reanudable, que llega con ~2 días de latencia. Es inmutable y se usa como fallback de ventanas SWPC multisatélite: la ventana entera se reintenta con un único satélite NCEI, nunca mezclando fuentes.

- Solo observaciones. **Sin vuelos, sin datos personales, sin decisiones de usuario.**
- Las capturas originales son inmutables. Una corrección posterior va en fichero aparte.
- La disponibilidad depende de las fuentes (SWPC/NCEI) y de la red; no se garantiza un servicio continuo.
