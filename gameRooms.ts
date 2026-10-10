import { randomBytes } from "node:crypto";
import type { Server as HttpServer } from "node:http";
import { Server, type Socket } from "socket.io";
import { logger } from "./lib/logger";

const ROOM_ALPHABET = "ABCDEFGHJKMNPQRSTUVWXYZ23456789";
const ROOM_SIZE = 6;
const MAX_PLAYERS = 4;
const MIN_PLAYERS_TO_START = 2;
const ROUND_SECONDS = 6;
const PLAYER_RESERVATION_MS = 45_000;
const EMPTY_ROOM_TTL_MS = 90_000;
const NEXT_ROUND_DELAY_MS = 2_500;

const ALLOWED_AVATARS = new Set([
  "Hollow Knight Cap",
  "Señor Coquetón",
  "Gumball Raro",
  "Darwin Gritando",
  "Steve Agujero Negro",
  "Atardecer Minecraft",
  "Rick en el Campo",
  "Perro Netherite",
  "Evil Morty",
  "Rick Bomba",
  "Caballero del Eco",
  "Máscara Celeste",
  "Viajero Umbral",
  "Nino Nakano Bruja",
  "Nino Nakano Corazón",
]);

type Phase = "lobby" | "playing" | "finished";
type PlayerResult = "correct" | "wrong" | null;
type RoundResult = "success" | "failure" | null;

interface Player {
  id: string;
  token: string;
  name: string;
  avatar: string;
  socketId: string | null;
  connected: boolean;
  reservationTimer: NodeJS.Timeout | null;
  result: PlayerResult;
}

interface Game {
  level: number;
  geo: number;
  masks: number;
  maxMasks: number;
  round: number;
  sequence: string | null;
  deadline: number | null;
  roundResult: RoundResult;
  notice: string;
  roundTimer: NodeJS.Timeout | null;
  nextRoundTimer: NodeJS.Timeout | null;
}

interface Room {
  code: string;
  phase: Phase;
  hostPlayerId: string;
  players: Map<string, Player>;
  game: Game;
  emptyTimer: NodeJS.Timeout | null;
}

interface ProfilePayload {
  name?: unknown;
  avatar?: unknown;
}

interface RoomCodePayload {
  code?: unknown;
}

interface ResumePayload extends RoomCodePayload {
  playerId?: unknown;
  playerToken?: unknown;
}

interface SubmitPayload {
  answer?: unknown;
}

interface AckResult {
  ok: boolean;
  error?: string;
  code?: string;
  playerId?: string;
  playerToken?: string;
}

type Ack = (result: AckResult) => void;

function randomId(bytes = 16): string {
  return randomBytes(bytes).toString("hex");
}

function makeRoomCode(): string {
  return Array.from(randomBytes(ROOM_SIZE), (byte) =>
    ROOM_ALPHABET[byte % ROOM_ALPHABET.length],
  ).join("");
}

function normalizeCode(code: unknown): string | null {
  if (typeof code !== "string") return null;
  const normalized = code.trim().toUpperCase();
  return /^[A-HJ-KM-NP-Z2-9]{6}$/.test(normalized) ? normalized : null;
}

function cleanName(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const name = value.replace(/[\u0000-\u001f\u007f]/g, "").trim().slice(0, 24);
  return name.length >= 1 ? name : null;
}

function cleanAvatar(value: unknown): string {
  return typeof value === "string" && ALLOWED_AVATARS.has(value)
    ? value
    : "Hollow Knight Cap";
}

function newGame(): Game {
  return {
    level: 1,
    geo: 0,
    masks: 5,
    maxMasks: 5,
    round: 0,
    sequence: null,
    deadline: null,
    roundResult: null,
    notice: "Esperando a que el anfitrión comience.",
    roundTimer: null,
    nextRoundTimer: null,
  };
}

function connectedCount(room: Room): number {
  return [...room.players.values()].filter((player) => player.connected).length;
}

function stopRoomTimers(room: Room): void {
  if (room.game.roundTimer) clearTimeout(room.game.roundTimer);
  if (room.game.nextRoundTimer) clearTimeout(room.game.nextRoundTimer);
  if (room.emptyTimer) clearTimeout(room.emptyTimer);
  room.game.roundTimer = null;
  room.game.nextRoundTimer = null;
  room.emptyTimer = null;
}

function generateSequence(level: number): string {
  const keys = ["W", "A", "S", "D"] as const;
  const length = Math.min(3 + level, 12);
  return Array.from(
    { length },
    () => keys[Math.floor(Math.random() * keys.length)],
  ).join("");
}

export function attachGameRooms(httpServer: HttpServer): Server {
  const io = new Server(httpServer, {
    path: "/socket.io",
    maxHttpBufferSize: 8_000,
    pingInterval: 25_000,
    pingTimeout: 20_000,
  });
  const rooms = new Map<string, Room>();

  function playerForSocket(socket: Socket): {
    room: Room;
    player: Player;
  } | null {
    const code = socket.data["roomCode"];
    const playerId = socket.data["playerId"];
    if (typeof code !== "string" || typeof playerId !== "string") return null;
    const room = rooms.get(code);
    const player = room?.players.get(playerId);
    return room && player ? { room, player } : null;
  }

  function publicState(room: Room, currentPlayerId: string) {
    return {
      code: room.code,
      phase: room.phase,
      hostPlayerId: room.hostPlayerId,
      playerId: currentPlayerId,
      players: [...room.players.values()].map((player) => ({
        id: player.id,
        name: player.name,
        avatar: player.avatar,
        connected: player.connected,
        isHost: player.id === room.hostPlayerId,
        submitted: player.result !== null,
        result: player.result,
      })),
      game: {
        level: room.game.level,
        geo: room.game.geo,
        masks: room.game.masks,
        maxMasks: room.game.maxMasks,
        sequence: room.game.sequence,
        deadline: room.game.deadline,
        roundResult: room.game.roundResult,
        notice: room.game.notice,
        round: room.game.round,
      },
    };
  }

  function broadcastState(room: Room): void {
    for (const player of room.players.values()) {
      if (player.connected && player.socketId) {
        io.to(player.socketId).emit("room:state", publicState(room, player.id));
      }
    }
  }

  function deleteRoom(room: Room): void {
    stopRoomTimers(room);
    for (const player of room.players.values()) {
      if (player.reservationTimer) clearTimeout(player.reservationTimer);
      const activeSocket = player.socketId
        ? io.sockets.sockets.get(player.socketId)
        : undefined;
      if (activeSocket) {
        activeSocket.leave(room.code);
        delete activeSocket.data["roomCode"];
        delete activeSocket.data["playerId"];
      }
    }
    rooms.delete(room.code);
    logger.info({ roomCode: room.code }, "Multiplayer room removed");
  }

  function scheduleEmptyCleanup(room: Room): void {
    if (room.emptyTimer) clearTimeout(room.emptyTimer);
    room.emptyTimer = setTimeout(() => {
      if (connectedCount(room) === 0) deleteRoom(room);
    }, EMPTY_ROOM_TTL_MS);
    room.emptyTimer.unref?.();
  }

  function promoteHost(room: Room): void {
    const currentHost = room.players.get(room.hostPlayerId);
    if (currentHost?.connected) return;
    const replacement = [...room.players.values()].find(
      (player) => player.connected,
    );
    if (replacement) {
      room.hostPlayerId = replacement.id;
      room.game.notice = `${replacement.name} ahora es anfitrión.`;
    }
  }

  function removePlayer(room: Room, playerId: string): void {
    const player = room.players.get(playerId);
    if (!player) return;
    if (player.reservationTimer) clearTimeout(player.reservationTimer);
    if (player.socketId) {
      const activeSocket = io.sockets.sockets.get(player.socketId);
      if (activeSocket) {
        activeSocket.leave(room.code);
        delete activeSocket.data["roomCode"];
        delete activeSocket.data["playerId"];
      }
    }
    room.players.delete(playerId);
    promoteHost(room);
    if (connectedCount(room) === 0) scheduleEmptyCleanup(room);
    broadcastState(room);
  }

  function reserveDisconnectedPlayer(room: Room, player: Player): void {
    player.connected = false;
    player.socketId = null;
    if (player.reservationTimer) clearTimeout(player.reservationTimer);
    player.reservationTimer = setTimeout(() => {
      removePlayer(room, player.id);
    }, PLAYER_RESERVATION_MS);
    player.reservationTimer.unref?.();

    promoteHost(room);
    if (connectedCount(room) === 0) {
      if (room.game.roundTimer) clearTimeout(room.game.roundTimer);
      room.game.roundTimer = null;
      room.game.deadline = null;
      scheduleEmptyCleanup(room);
    }
    broadcastState(room);
  }

  function scheduleRoundTimeout(room: Room): void {
    if (room.game.roundTimer) clearTimeout(room.game.roundTimer);
    if (room.game.deadline === null) return;
    const remaining = Math.max(0, room.game.deadline - Date.now());
    room.game.roundTimer = setTimeout(() => finishRound(room, false), remaining);
    room.game.roundTimer.unref?.();
  }

  function startNextRound(room: Room): void {
    if (room.phase !== "playing") return;
    room.game.round += 1;
    room.game.sequence = generateSequence(room.game.level);
    room.game.deadline = Date.now() + ROUND_SECONDS * 1_000;
    room.game.roundResult = null;
    room.game.notice = `Ronda ${room.game.round}: memoricen la secuencia y colaboren.`;
    for (const player of room.players.values()) player.result = null;
    scheduleRoundTimeout(room);
    broadcastState(room);
  }

  function finishRound(room: Room, success: boolean): void {
    if (room.phase !== "playing" || room.game.roundResult !== null) return;
    if (room.game.roundTimer) clearTimeout(room.game.roundTimer);
    room.game.roundTimer = null;
    room.game.deadline = Date.now();
    room.game.roundResult = success ? "success" : "failure";

    if (success) {
      const reward = Math.floor(15 + Math.random() * 16) + room.game.level * 8;
      room.game.geo += reward;
      room.game.level += 1;
      room.game.notice = `¡Secuencia completada! El equipo ganó ${reward} Geo. Nivel ${room.game.level}.`;
      if (Math.random() < 0.12 && room.game.masks < room.game.maxMasks) {
        room.game.masks += 1;
        room.game.notice += " Encontraron una máscara.";
      }
      if (Math.random() < 0.25) {
        const extra = 10 + Math.floor(Math.random() * 16);
        room.game.geo += extra;
        room.game.notice += ` Hallaron ${extra} Geo extra.`;
      }
    } else {
      room.game.masks = Math.max(0, room.game.masks - 1);
      room.game.level = Math.max(1, room.game.level - 1);
      room.game.notice = "La secuencia no se completó. El equipo perdió una máscara.";
      if (room.game.masks === 0) {
        room.phase = "finished";
        room.game.notice += " El equipo se quedó sin máscaras.";
      }
    }

    broadcastState(room);
    if (room.phase === "playing") {
      room.game.nextRoundTimer = setTimeout(
        () => startNextRound(room),
        NEXT_ROUND_DELAY_MS,
      );
      room.game.nextRoundTimer.unref?.();
    }
  }

  function replyWithError(
    socket: Socket,
    ack: Ack | undefined,
    message: string,
  ): void {
    ack?.({ ok: false, error: message });
    socket.emit("room:error", { message });
  }

  function detachFromPreviousRoom(socket: Socket): void {
    const membership = playerForSocket(socket);
    if (membership) removePlayer(membership.room, membership.player.id);
  }

  function bindPlayer(
    socket: Socket,
    room: Room,
    player: Player,
  ): void {
    detachFromPreviousRoom(socket);
    if (room.emptyTimer) {
      clearTimeout(room.emptyTimer);
      room.emptyTimer = null;
    }
    if (player.reservationTimer) {
      clearTimeout(player.reservationTimer);
      player.reservationTimer = null;
    }

    const previousSocketId = player.socketId;
    if (previousSocketId && previousSocketId !== socket.id) {
      io.sockets.sockets.get(previousSocketId)?.disconnect(true);
    }
    player.connected = true;
    player.socketId = socket.id;
    socket.data["roomCode"] = room.code;
    socket.data["playerId"] = player.id;
    void socket.join(room.code);
    promoteHost(room);

    if (
      room.phase === "playing" &&
      room.game.sequence !== null &&
      room.game.deadline === null
    ) {
      room.game.deadline = Date.now() + ROUND_SECONDS * 1_000;
      scheduleRoundTimeout(room);
    }

    broadcastState(room);
  }

  io.on("connection", (socket) => {
    socket.on(
      "room:create",
      (payload: ProfilePayload, ack?: Ack) => {
        detachFromPreviousRoom(socket);
        const name = cleanName(payload?.name);
        if (!name) {
          replyWithError(socket, ack, "Escribe un nombre para crear la sala.");
          return;
        }

        let code = makeRoomCode();
        while (rooms.has(code)) code = makeRoomCode();
        const player: Player = {
          id: randomId(8),
          token: randomId(24),
          name,
          avatar: cleanAvatar(payload.avatar),
          socketId: null,
          connected: false,
          reservationTimer: null,
          result: null,
        };
        const room: Room = {
          code,
          phase: "lobby",
          hostPlayerId: player.id,
          players: new Map([[player.id, player]]),
          game: newGame(),
          emptyTimer: null,
        };
        rooms.set(code, room);
        bindPlayer(socket, room, player);
        ack?.({
          ok: true,
          code,
          playerId: player.id,
          playerToken: player.token,
        });
        logger.info({ roomCode: code }, "Multiplayer room created");
      },
    );

    socket.on(
      "room:join",
      (payload: ProfilePayload & RoomCodePayload, ack?: Ack) => {
        detachFromPreviousRoom(socket);
        const code = normalizeCode(payload?.code);
        const name = cleanName(payload?.name);
        if (!code || !name) {
          replyWithError(socket, ack, "Revisa el código y tu nombre.");
          return;
        }
        const room = rooms.get(code);
        if (!room) {
          replyWithError(socket, ack, "No existe una sala con ese código.");
          return;
        }
        if (room.phase !== "lobby") {
          replyWithError(socket, ack, "La partida ya comenzó.");
          return;
        }
        if (room.players.size >= MAX_PLAYERS) {
          replyWithError(socket, ack, "La sala ya tiene cuatro jugadores.");
          return;
        }

        const player: Player = {
          id: randomId(8),
          token: randomId(24),
          name,
          avatar: cleanAvatar(payload.avatar),
          socketId: null,
          connected: false,
          reservationTimer: null,
          result: null,
        };
        room.players.set(player.id, player);
        bindPlayer(socket, room, player);
        ack?.({
          ok: true,
          code,
          playerId: player.id,
          playerToken: player.token,
        });
      },
    );

    socket.on(
      "room:resume",
      (payload: ResumePayload, ack?: Ack) => {
        const code = normalizeCode(payload?.code);
        const playerId =
          typeof payload?.playerId === "string" ? payload.playerId : "";
        const playerToken =
          typeof payload?.playerToken === "string" ? payload.playerToken : "";
        const room = code ? rooms.get(code) : undefined;
        const player = room?.players.get(playerId);
        if (!room || !player || player.token !== playerToken) {
          replyWithError(socket, ack, "No se pudo recuperar la sala; vuelve a unirte.");
          return;
        }
        bindPlayer(socket, room, player);
        ack?.({ ok: true, code: room.code, playerId, playerToken });
      },
    );

    socket.on("room:start", (ack?: Ack) => {
      const membership = playerForSocket(socket);
      if (!membership) {
        replyWithError(socket, ack, "Primero debes entrar a una sala.");
        return;
      }
      const { room, player } = membership;
      if (room.phase !== "lobby") {
        replyWithError(socket, ack, "La partida ya comenzó.");
        return;
      }
      if (player.id !== room.hostPlayerId) {
        replyWithError(socket, ack, "Solo el anfitrión puede comenzar.");
        return;
      }
      if (connectedCount(room) < MIN_PLAYERS_TO_START) {
        replyWithError(socket, ack, "Se necesitan al menos dos jugadores conectados.");
        return;
      }
      room.phase = "playing";
      room.game.notice = "La aventura cooperativa ha comenzado.";
      ack?.({ ok: true });
      startNextRound(room);
    });

    socket.on(
      "round:submit",
      (payload: SubmitPayload, ack?: Ack) => {
        const membership = playerForSocket(socket);
        if (!membership) {
          replyWithError(socket, ack, "Primero debes entrar a una sala.");
          return;
        }
        const { room, player } = membership;
        if (
          room.phase !== "playing" ||
          room.game.roundResult !== null ||
          room.game.sequence === null
        ) {
          replyWithError(socket, ack, "No hay una secuencia activa.");
          return;
        }
        if (!player.connected) {
          replyWithError(socket, ack, "Reconéctate para responder.");
          return;
        }
        if (player.result !== null) {
          replyWithError(socket, ack, "Ya enviaste tu respuesta en esta ronda.");
          return;
        }
        if (room.game.deadline !== null && Date.now() >= room.game.deadline) {
          finishRound(room, false);
          ack?.({ ok: false, error: "Se acabó el tiempo." });
          return;
        }
        const answer =
          typeof payload?.answer === "string"
            ? payload.answer.toUpperCase().replace(/\s/g, "").slice(0, 12)
            : "";
        if (!answer) {
          replyWithError(socket, ack, "Escribe la secuencia antes de enviarla.");
          return;
        }

        player.result = answer === room.game.sequence ? "correct" : "wrong";
        ack?.({ ok: true });
        if (player.result === "correct") {
          finishRound(room, true);
          return;
        }

        const connectedPlayers = [...room.players.values()].filter(
          (roomPlayer) => roomPlayer.connected,
        );
        if (
          connectedPlayers.length > 0 &&
          connectedPlayers.every((roomPlayer) => roomPlayer.result === "wrong")
        ) {
          finishRound(room, false);
          return;
        }
        room.game.notice = `${player.name} ya respondió. Esperando al resto del equipo.`;
        broadcastState(room);
      },
    );

    socket.on("room:leave", (ack?: Ack) => {
      const membership = playerForSocket(socket);
      if (membership) removePlayer(membership.room, membership.player.id);
      ack?.({ ok: true });
    });

    socket.on("disconnect", () => {
      const membership = playerForSocket(socket);
      if (membership) reserveDisconnectedPlayer(membership.room, membership.player);
    });
  });

  io.engine.on("connection_error", (error) => {
    logger.warn(
      { code: error.code, message: error.message },
      "Socket.IO connection rejected",
    );
  });

  return io;
}
