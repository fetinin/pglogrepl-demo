const wsChannelDefault = "changes";
const wsUrlBase = "ws://localhost:7700/listen/";

// Set up the defaults and hook up the events
// once the page is finished loading.
window.onload = function () {
    wsConnect();
}

// When the channel connect button is clicked
// (and at the end of the page load routine)
// we connect to the event server.
function wsConnect() {
    const wsUrl = wsUrlBase + wsChannelDefault;

    const outputStatus = document.getElementById("status");
    const outputMessage = document.getElementById("textarea");

    const ws = new WebSocket(wsUrl);
    ws.onopen = function () {
        outputStatus.innerHTML = `Connected to "${wsChannelDefault}".`;
    };

    ws.onerror = function (error) {
        console.log("WebSocket error: " + error.message);
    };

    ws.onclose = function () {
        outputStatus.innerHTML = `Disconnected.`;
    };

    // Got a message from the WebSocket!
    ws.onmessage = function (e) {
        outputMessage.value += e.data + "\n";
    };
}
