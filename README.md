# cosmic-rad-data

Archivo público de observaciones de protones solares (GOES vía SWPC) usado por [cosmic-rad](https://github.com/ibpilot/cosmic-rad).

- Solo observaciones. **Sin vuelos, sin datos personales, sin decisiones de usuario.**
- Estructura: `solar/YYYY/MM/YYYY-MM-DD.json`, un fichero por día UTC completo.
- `manifest.json` publica último éxito, cobertura, días incompletos, satélites y versión.
- Las capturas originales son inmutables. Una corrección posterior va en fichero aparte.

Recolectado automáticamente cada 6 h. SWPC solo sirve 7 días: los huecos se recuperan dentro de esa ventana y después son permanentes.
