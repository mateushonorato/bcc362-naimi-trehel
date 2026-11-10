// Cria o elemento de áudio uma vez
const alarmSound = new Audio('/static/alarm.mp3');
alarmSound.volume = 0.5;

let lastBlockedCount = 0;
let alarmCooldown = false;

function checkAlarm(blockedCount) {
    if (blockedCount - lastBlockedCount >= 5 && !alarmCooldown) {
        alarmCooldown = true;
        alarmSound.play().catch(e => console.log("Áudio bloqueado:", e));
        setTimeout(() => { alarmCooldown = false; }, 3000);
    }
    lastBlockedCount = blockedCount;
}