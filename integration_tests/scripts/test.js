const fs = require('fs');
const crypto = require('crypto');

const MAX_BLOB = 256n;

async function loop() {
    let i = 0;
    while (true) {
        if (i > MAX_BLOB) {
            break;
        }
        const buf = crypto.randomBytes(126976);
        fs.appendFile(".data", buf.toString('hex'));
        i++;
    }
}

loop();

