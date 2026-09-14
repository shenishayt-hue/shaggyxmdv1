import express from 'express';
import bodyParser from 'body-parser';
import path from 'path';
import { fileURLToPath } from 'url';
import { EventEmitter } from 'events';  

const app = express();
const __filename = fileURLToPath(import.meta.url);
const __path = path.dirname(__filename);
const PORT = process.env.PORT || 8000;
import { router as code } from './pair.js';
EventEmitter.defaultMaxListeners = 500;
app.use('/code', code);
app.use('/pair', async (req, res, next) => {
    res.sendFile(path.join(__path, 'pair.html'));
});

app.use('/', async (req, res, next) => {
    res.sendFile(path.join(__path, 'main.html'));
});

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

app.listen(PORT, () => {
    console.log(`
 ██████╗  ██╗  ██╗ ██████╗ ███████╗████████╗
██╔════╝  ██║  ██║██╔═══██╗██╔════╝╚══██╔══╝
██║  ███╗ ███████║██║   ██║███████╗   ██║   
██║   ██║ ██╔══██║██║   ██║╚════██║   ██║   
╚██████╔╝ ██║  ██║╚██████╔╝███████║   ██║   
 ╚═════╝  ╚═╝  ╚═╝ ╚═════╝ ╚══════╝   ╚═╝   

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
💖 Don't Forget To Give a Star
🌐 URL    : http://localhost:${PORT}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
`);
});

export default app;
