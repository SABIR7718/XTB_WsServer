/*
 * © 2026 SeXyxeon (VOIDSEC)
 *
 * ⚠️ COPYRIGHT NOTICE
 * This source code is protected under copyright law.
 * Any form of re-uploading, recoding, modification,
 * selling, or redistribution WITHOUT explicit permission
 * from the original author is strictly prohibited.
 *
 * ❌ NO CREDIT = NO PERMISSION
 * ❌ DO NOT CLAIM THIS CODE AS YOUR OWN
 *
 * ✔️ Usage or modification is allowed ONLY
 * with prior permission and proper credit.
 *
 * OFFICIAL LINKS (ONLY):
 * YouTube   : https://youtube.com/@voidsec7718
 * Instagram : sabir._7718
 * Telegram  : https://t.me/SABIR7718
 * GitHub    : https://github.com/SABIR7718
 * WhatsApp  : +91 73650 85213
 *
 * Violations may result in DMCA takedown
 * or termination of the Telegram bot.
 */

require("dotenv").config();

const WebSocket = require("ws");
const crypto = require("crypto");
const {
    log
} = require("@sabir7718/log");

const SABIR7718_SERVERS = new Map();
const SYHaTE_CLIENTS = new Map();
const SYHaTeConnectionState = new Map();

const S7_PORT = process.env.PORT || 8080;
const SYHaTeS7_RELAY_SECRET =
    process.env.RELAY_SECRET || "SABIR7718_RELAY_SECRET";

function S7HaTe_ID() {
    return crypto.randomBytes(12).toString("hex");
}

function SYHaTeS7_SEND(S7HaTe_CS, SABIR7718_PAYLOAD) {
    if (
        S7HaTe_CS &&
        S7HaTe_CS.readyState === WebSocket.OPEN
    ) {
        SABIR7718_PAYLOAD.Developer = "SABIR7718";

        S7HaTe_CS.send(
            JSON.stringify(SABIR7718_PAYLOAD)
        );

        return true;
    }

    return false;
}

function SABIR7718_NOTIFY_CLIENTS(
    SYHaTE_SERVER_NAME,
    S7_PAYLOAD
) {
    for (
        const [
            S7HaTe_CS,
            SYHaTE_CLIENT_DATA
        ] of SYHaTE_CLIENTS
    ) {
        if (
            SYHaTE_CLIENT_DATA.servers.has(
                SYHaTE_SERVER_NAME
            )
        ) {
            SYHaTeS7_SEND(
                S7HaTe_CS,
                S7_PAYLOAD
            );
        }
    }
}

function S7_SUBSCRIBE_CLIENT(
    SYHaTeS7_CS,
    S7_SERVER_NAME
) {
    const SABIR7718_CLIENT_DATA =
        SYHaTE_CLIENTS.get(
            SYHaTeS7_CS
        );

    if (!SABIR7718_CLIENT_DATA) {
        return;
    }

    SABIR7718_CLIENT_DATA.servers.add(
        S7_SERVER_NAME
    );
}

function SABIR7718_GET_SERVER_STATUS() {
    const SYHaTeServerList = [];

    for (
        const [
            SYHaTeServerName,
            SYHaTeServerData
        ] of SABIR7718_SERVERS
    ) {
        const SYHaTeServerSocket =
            SYHaTeServerData.ws;

        const SYHaTeConnected =
            Boolean(
                SYHaTeServerSocket &&
                SYHaTeServerSocket.readyState ===
                WebSocket.OPEN
            );

        SYHaTeServerList.push({
            name: SYHaTeServerName,
            key: SYHaTeServerName,
            connected: SYHaTeConnected,
            readyState: SYHaTeServerSocket ?
                SYHaTeServerSocket.readyState : null,
            connectedAt: SYHaTeServerData.connectedAt,
            uptime: SYHaTeServerData.connectedAt ?
                Date.now() -
                SYHaTeServerData.connectedAt : null
        });
    }

    const SYHaTeConnectedCount =
        SYHaTeServerList.filter(
            (SYHaTeServer) =>
            SYHaTeServer.connected
        ).length;

    return {
        count: SYHaTeConnectedCount,
        total: SYHaTeServerList.length,
        servers: SYHaTeServerList
    };
}

const SYHaTe_WSS = new WebSocket.Server({
    port: S7_PORT
});

SYHaTe_WSS.on(
    "connection",
    (S7HaTe_CS) => {
        let SYHaTE_TYPE = null;
        let SABIR7718_SERVER_NAME = null;

        log(
            "info",
            "WEBSOCKET",
            "New connection"
        );

        S7HaTe_CS.on(
            "message",
            (SYHaTe_RAW) => {
                try {
                    const S7_REQUEST =
                        JSON.parse(
                            SYHaTe_RAW.toString()
                        );

                    if (!SYHaTE_TYPE) {
                        if (
                            S7_REQUEST.type ===
                            "server"
                        ) {
                            const {
                                name,
                                key
                            } = S7_REQUEST;

                            if (
                                !name ||
                                !key
                            ) {
                                return SYHaTeS7_SEND(
                                    S7HaTe_CS, {
                                        success: false,
                                        error: "Server name and key required"
                                    }
                                );
                            }

                            if (
                                key !==
                                SYHaTeS7_RELAY_SECRET
                            ) {
                                SYHaTeS7_SEND(
                                    S7HaTe_CS, {
                                        success: false,
                                        error: "Invalid server key"
                                    }
                                );

                                return S7HaTe_CS.close();
                            }

                            const S7_OLD_SERVER =
                                SABIR7718_SERVERS.get(
                                    name
                                );

                            if (
                                S7_OLD_SERVER &&
                                S7_OLD_SERVER.ws !==
                                S7HaTe_CS
                            ) {
                                try {
                                    S7_OLD_SERVER.ws.close();
                                } catch {}
                            }

                            SABIR7718_SERVERS.set(
                                name, {
                                    ws: S7HaTe_CS,
                                    connectedAt: Date.now()
                                }
                            );

                            SYHaTE_TYPE =
                                "server";

                            SABIR7718_SERVER_NAME =
                                name;

                            log(
                                "success",
                                "SERVER",
                                `${name} connected`
                            );

                            SABIR7718_NOTIFY_CLIENTS(
                                name, {
                                    event: "serverConnected",
                                    server: name,
                                    key: name,
                                    connected: true
                                }
                            );

                            return SYHaTeS7_SEND(
                                S7HaTe_CS, {
                                    success: true,
                                    event: "registered",
                                    server: name,
                                    key: name
                                }
                            );
                        }

                        if (S7_REQUEST.type === "client") {
                            if (!S7_REQUEST.key) {
                                return SYHaTeS7_SEND(S7HaTe_CS, {
                                    success: false,
                                    error: "Client key required"
                                });
                            }

                            if (S7_REQUEST.key !== process.env.CLIENT_SECRET) {
                                SYHaTeS7_SEND(S7HaTe_CS, {
                                    success: false,
                                    error: "Invalid client key"
                                });

                                return S7HaTe_CS.close();
                            }

                            SYHaTE_TYPE = "client";

                            SYHaTE_CLIENTS.set(S7HaTe_CS, {
                                servers: new Set(),
                                authenticatedAt: Date.now()
                            });

                            log("success", "CLIENT", "Authenticated client connected");

                            return SYHaTeS7_SEND(S7HaTe_CS, {
                                success: true,
                                event: "connected"
                            });
                        }

                        return SYHaTeS7_SEND(
                            S7HaTe_CS, {
                                success: false,
                                error: "First message must register as server or client"
                            }
                        );
                    }

                    if (
                        SYHaTE_TYPE ===
                        "client"
                    ) {
                        if (
                            S7_REQUEST.method ===
                            "servers"
                        ) {
                            for (
                                const SYHaTeServerName of SABIR7718_SERVERS.keys()
                            ) {
                                S7_SUBSCRIBE_CLIENT(
                                    S7HaTe_CS,
                                    SYHaTeServerName
                                );
                            }

                            return SYHaTeS7_SEND(
                                S7HaTe_CS, {
                                    id: S7_REQUEST.id,
                                    success: true,
                                    event: "serverStatus",
                                    ...SABIR7718_GET_SERVER_STATUS()
                                }
                            );
                        }

                        const S7_TARGET =
                            S7_REQUEST.server;

                        if (!S7_TARGET) {
                            return SYHaTeS7_SEND(
                                S7HaTe_CS, {
                                    id: S7_REQUEST.id,
                                    success: false,
                                    error: "Server key required"
                                }
                            );
                        }

                        S7_SUBSCRIBE_CLIENT(
                            S7HaTe_CS,
                            S7_TARGET
                        );

                        const SYHaTeS7_SERVER =
                            SABIR7718_SERVERS.get(
                                S7_TARGET
                            );

                        if (
                            !SYHaTeS7_SERVER ||
                            !SYHaTeS7_SERVER.ws ||
                            SYHaTeS7_SERVER.ws.readyState !==
                            WebSocket.OPEN
                        ) {
                            return SYHaTeS7_SEND(
                                S7HaTe_CS, {
                                    id: S7_REQUEST.id,
                                    success: false,
                                    event: "serverDisconnected",
                                    server: S7_TARGET,
                                    key: S7_TARGET,
                                    connected: false,
                                    error: "Server is disconnected"
                                }
                            );
                        }

                        const SABIR7718_REQUEST_ID =
                            S7_REQUEST.id ||
                            S7HaTe_ID();

                        SYHaTeS7_SEND(
                            SYHaTeS7_SERVER.ws, {
                                type: "request",
                                id: SABIR7718_REQUEST_ID,
                                server: S7_TARGET,
                                method: S7_REQUEST.method,
                                number: S7_REQUEST.number,
                                args: S7_REQUEST.args || {}
                            }
                        );

                        return;
                    }

                    if (
                        SYHaTE_TYPE ===
                        "server"
                    ) {
                        if (
                            S7_REQUEST.type !==
                            "response"
                        ) {
                            return SYHaTeS7_SEND(
                                S7HaTe_CS, {
                                    success: false,
                                    error: "Only response messages are accepted"
                                }
                            );
                        }

                        const S7_RESPONSE_ID =
                            S7_REQUEST.id;

                        SABIR7718_NOTIFY_CLIENTS(
                            SABIR7718_SERVER_NAME, {
                                ...S7_REQUEST,
                                type: "response",
                                server: SABIR7718_SERVER_NAME,
                                Developer: "SABIR7718"
                            }
                        );
                    }
                } catch (
                    SYHaTeS7_ERROR
                ) {
                    SYHaTeS7_SEND(
                        S7HaTe_CS, {
                            success: false,
                            error: "Invalid JSON"
                        }
                    );

                    log(
                        "error",
                        "WEBSOCKET",
                        SYHaTeS7_ERROR.message
                    );
                }
            }
        );

        S7HaTe_CS.on(
            "close",
            () => {
                if (
                    SYHaTE_TYPE ===
                    "server"
                ) {
                    const S7_CURRENT =
                        SABIR7718_SERVERS.get(
                            SABIR7718_SERVER_NAME
                        );

                    if (
                        S7_CURRENT &&
                        S7_CURRENT.ws ===
                        S7HaTe_CS
                    ) {
                        SABIR7718_SERVERS.delete(
                            SABIR7718_SERVER_NAME
                        );

                        SABIR7718_NOTIFY_CLIENTS(
                            SABIR7718_SERVER_NAME, {
                                event: "serverDisconnected",
                                server: SABIR7718_SERVER_NAME,
                                key: SABIR7718_SERVER_NAME,
                                connected: false
                            }
                        );

                        log(
                            "warn",
                            "SERVER",
                            `${SABIR7718_SERVER_NAME} disconnected`
                        );
                    }
                }

                if (
                    SYHaTE_TYPE ===
                    "client"
                ) {
                    SYHaTE_CLIENTS.delete(
                        S7HaTe_CS
                    );

                    log(
                        "warn",
                        "CLIENT",
                        "Client disconnected"
                    );
                }
            }
        );

        S7HaTe_CS.on(
            "error",
            (SABIR7718_ERROR) => {
                log(
                    "error",
                    "WEBSOCKET",
                    SABIR7718_ERROR.message
                );
            }
        );
    }
);

SYHaTe_WSS.on(
    "listening",
    () => {
        log(
            "success",
            "WEBSOCKET",
            `S7 Relay server listening on port ${S7_PORT}`
        );
    }
);

if (process.env.URL) {
    (async () => {
        try {
            const res =
                await fetch(
                    process.env.URL
                );

            log(
                "info",
                "PING",
                `Pinged: ${process.env.URL} | Status: ${res.status}`
            );
        } catch (
            err
        ) {
            log(
                "error",
                "PING",
                err.message
            );
        }
    })();

    setInterval(
        async () => {
                try {
                    const res =
                        await fetch(
                            process.env.URL
                        );

                    log(
                        "info",
                        "PING",
                        `Pinged: ${process.env.URL} | Status: ${res.status}`
                    );
                } catch (
                    err
                ) {
                    log(
                        "error",
                        "PING",
                        err.message
                    );
                }
            },
            5 * 60 * 1000
    );
}