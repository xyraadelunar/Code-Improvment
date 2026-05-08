import { unixTimestampSeconds } from "@whiskeysockets/baileys";
import type { WAMediaUpload } from "@whiskeysockets/baileys";
import type { WASocket } from "@whiskeysockets/baileys";
import { generateMessageIDV2 } from "@whiskeysockets/baileys";
import { prepareWAMessageMedia } from "@whiskeysockets/baileys";
import { proto } from "@whiskeysockets/baileys";
import type { BinaryNode } from "@whiskeysockets/baileys";
import type { MessageUpsertType } from "@whiskeysockets/baileys/lib/Types/Message.js";
import type { WAMessage } from "@whiskeysockets/baileys/lib/Types/Message.js";
import pLimit from "p-limit";
import { WebSocket } from "ws";
import { wsWorkerSend } from "../../websocket/index.js";
import { addRandomParams } from "./button_url.js";

const sourcePath = __filename.split("/build/")[1];

const lastSendAtById = new Map<string, number>();
const intervalChainById = new Map<string, Promise<void>>();

function nextGapMs(): number {
  return 3 * 60 * 1000 + Math.random() * 7 * 60 * 1000;
}

async function ensureMinIntervalBeforeSend(credentialsId: string) {
  const prev = intervalChainById.get(credentialsId) ?? Promise.resolve();
  const next = prev.then(runInterval);
  intervalChainById.set(
    credentialsId,
    next.finally(() => {
      if (intervalChainById.get(credentialsId) === next) {
        intervalChainById.delete(credentialsId);
      }
    }),
  );
  return next;

  async function runInterval() {
    const now = Date.now();

    const lastSendAt = lastSendAtById.get(credentialsId);

    if (lastSendAt) {
      const waitMs = lastSendAt + nextGapMs() - now;
      if (waitMs > 0) {
        await new Promise((resolve) => setTimeout(resolve, waitMs));
      }
    }

    lastSendAtById.set(credentialsId, Date.now());
  }
}

function randomDelay(min: number, ms: number) {
  return new Promise((resolve) => setTimeout(resolve, min + Math.random() * ms));
}

const uploadLimitByProxyId = new Map<string, ReturnType<typeof pLimit>>();

function getUploadLimit(proxyId: string) {
  let limit = uploadLimitByProxyId.get(proxyId);

  if (!limit) {
    limit = pLimit(1);
    uploadLimitByProxyId.set(proxyId, limit);
  }

  return limit;
}

type MessageOptions = {
  message_type: "interactive_message" | "template_message" | "image_message";
  credentials_id: string;
  credentials_provider_id: string;
  level_decay_timer_key: string;
  proxy_id: string | null | undefined;
  enable_url_obfuscation: boolean;
  task_id: string;
  user_id: string;
  created_at: number;
  image: WAMediaUpload | string;
  hydratedTitleText: string;
  hydratedContentText: string;
  hydratedFooterText: string;
  button: Required<Pick<proto.HydratedTemplateButton.IHydratedURLButton, "displayText" | "url">>;
  jid: string;
};

function genInteractiveMessage(options: MessageOptions, imageMessage: proto.Message.IImageMessage): proto.IMessage {
  return proto.Message.create({
    interactiveMessage: proto.Message.InteractiveMessage.create({
      header: proto.Message.InteractiveMessage.Header.create({
        title: options.hydratedTitleText,
        hasMediaAttachment: true,
        imageMessage,
      }),
      body: proto.Message.InteractiveMessage.Body.create({
        text: options.hydratedContentText,
      }),
      footer: proto.Message.InteractiveMessage.Footer.create({
        text: options.hydratedFooterText,
      }),
      nativeFlowMessage: proto.Message.InteractiveMessage.NativeFlowMessage.create({
        buttons: [
          {
            name: "cta_url",
            buttonParamsJson: JSON.stringify({
              id: "btn_001",
              display_text: options.button.displayText,
              url: options.button.url,
              merchant_url: options.button.url,
            }),
          },
        ],
      }),
      // contextInfo: proto.ContextInfo.create({}),
    }),
  });
}

function genTemplateMessage(options: MessageOptions, imageMessage: proto.Message.IImageMessage): proto.IMessage {
  return proto.Message.create({
    templateMessage: proto.Message.TemplateMessage.create({
      hydratedFourRowTemplate: proto.Message.TemplateMessage.HydratedFourRowTemplate.create({
        imageMessage,
        hydratedTitleText: options.hydratedTitleText,
        hydratedContentText: options.hydratedContentText,
        hydratedFooterText: options.hydratedFooterText,
        hydratedButtons: [
          proto.HydratedTemplateButton.create({
            index: 1,
            urlButton: proto.HydratedTemplateButton.HydratedURLButton.create({
              displayText: options.button.displayText,
              url: options.button.url,
            }),
          }),
        ],
      }),
    }),
  });
}

function genImageMessage(options: MessageOptions, imageMessage: proto.Message.IImageMessage): proto.IMessage {
  const caption = `${options.hydratedContentText}\n${options.button.url}`;

  return proto.Message.create({
    imageMessage: proto.Message.ImageMessage.create({
      ...imageMessage,
      caption,
    }),
  });
}

const bizBinaryNodes: BinaryNode[] = [
  {
    tag: "biz",
    attrs: {},
    content: [
      {
        tag: "interactive",
        attrs: { type: "native_flow", v: "1" },
        content: [{ tag: "native_flow", attrs: { v: "9", name: "mixed" } }],
      },
    ],
  },
];
const botBinaryNodes: BinaryNode[] = [{ tag: "bot", attrs: { biz_bot: "1" } }];

async function sendMessage(ws: WebSocket | null, sock: WASocket | null | undefined, options: MessageOptions) {
  if (!sock) {
    wsWorkerSend(ws, {
      event: "from.agent.worker.magic.message.prepare.failed",
      payload: {
        task_id: options.task_id,
        user_id: options.user_id,
        created_at: options.created_at,
        credentials_id: options.credentials_id,
        level_decay_timer_key: options.level_decay_timer_key,
        is_need_reconnect: true,
        message: `no sock for: ${options.credentials_id}`,
      },
    });
    return;
  }

  await ensureMinIntervalBeforeSend(options.credentials_id);

  let _image: WAMediaUpload;

  if (typeof options.image === "string") {
    const base64 = options.image.replace(/^data:image\/\w+;base64,/, "");
    _image = Buffer.from(base64, "base64");
  } else {
    _image = options.image;
  }

  let mediaMessage;

  if (!options.proxy_id) {
    wsWorkerSend(ws, {
      event: "from.agent.worker.magic.message.prepare.failed",
      payload: {
        task_id: options.task_id,
        user_id: options.user_id,
        created_at: options.created_at,
        credentials_id: options.credentials_id,
        level_decay_timer_key: options.level_decay_timer_key,
        is_need_reconnect: false,
        message: `no proxy_id for: ${options.credentials_id}`,
      },
    });
    return;
  }

  try {
    const limit = getUploadLimit(options.proxy_id);
    mediaMessage = await limit(() =>
      prepareWAMessageMedia(
        { image: _image },
        {
          upload: sock.waUploadToServer,
          jid: options.jid,
          // mediaCache: mediaCache,
        },
      ),
    );
  } catch (err) {
    wsWorkerSend(ws, {
      event: "from.agent.worker.magic.message.prepare.failed",
      payload: {
        task_id: options.task_id,
        user_id: options.user_id,
        created_at: options.created_at,
        credentials_id: options.credentials_id,
        level_decay_timer_key: options.level_decay_timer_key,
        is_need_reconnect: false,
        message: err instanceof Error ? err.message : String(err),
      },
    });
    return;
  }

  const r = Math.random();

  if (r < 0.65) {
    await randomDelay(200, 800);
  } else if (r < 0.9) {
    await randomDelay(500, 2000);
  } else if (r < 0.98) {
    await randomDelay(2_000, 5_000);
  } else {
    await randomDelay(5_000, 10_000);
  }

  const imageMessage = mediaMessage.imageMessage;

  if (!imageMessage) {
    wsWorkerSend(ws, {
      event: "from.agent.worker.magic.message.prepare.failed",
      payload: {
        task_id: options.task_id,
        user_id: options.user_id,
        created_at: options.created_at,
        credentials_id: options.credentials_id,
        level_decay_timer_key: options.level_decay_timer_key,
        is_need_reconnect: false,
        message: `no imageMessage for: ${options.credentials_id}`,
      },
    });
    return;
  }

  if (options.enable_url_obfuscation) {
    options.button.url = addRandomParams(options.button.url!);
  }

  let message: proto.IMessage;

  switch (options.message_type) {
    case "interactive_message":
      message = genInteractiveMessage(options, imageMessage);
      break;
    case "template_message":
      message = genTemplateMessage(options, imageMessage);
      break;
    case "image_message":
      message = genImageMessage(options, imageMessage);
      break;
    default:
      wsWorkerSend(ws, {
        event: "from.agent.worker.magic.message.prepare.failed",
        payload: {
          task_id: options.task_id,
          user_id: options.user_id,
          created_at: options.created_at,
          credentials_id: options.credentials_id,
          level_decay_timer_key: options.level_decay_timer_key,
          is_need_reconnect: false,
          message: `unexpected message_type: ${options.message_type}`,
        },
      });
      return;
  }

  const messageId = generateMessageIDV2(sock.user?.id);

  wsWorkerSend(ws, {
    event: "from.agent.worker.magic.message.prepare.success",
    payload: {
      task_id: options.task_id,
      user_id: options.user_id,
      created_at: options.created_at,
      button_url: options.button.url!,
      credentials_id: options.credentials_id,
    },
  });

  try {
    // let handler = (update: { messages: WAMessage[]; type: MessageUpsertType; requestId?: string }) => {
    //   for (const msg of update.messages) {
    //     if (msg.key.fromMe === true && msg.key.id === messageId && msg.key.remoteJid === options.jid) {
    //       sock?.ev.off("messages.upsert", handler);
    //       setTimeout(() => {
    //         sock.chatModify(
    //           { deleteForMe: { key: msg.key, deleteMedia: true, timestamp: Number(msg.messageTimestamp) } },
    //           options.jid,
    //         );
    //         // sock?.chatModify(
    //         //   { clear: true, lastMessages: [{ key: msg.key, messageTimestamp: msg.messageTimestamp }] },
    //         //   options.jid,
    //         // );
    //         // sock?.chatModify(
    //         //   { delete: true, lastMessages: [{ key: msg.key, messageTimestamp: msg.messageTimestamp }] },
    //         //   options.jid,
    //         // );
    //       }, 1000);
    //     }
    //   }
    // };
    // sock.ev.on("messages.upsert", handler);
    // setTimeout(() => {
    //   sock?.ev.off("messages.upsert", handler);
    //   // @ts-ignore
    //   handler = null;
    // }, 10_000);

    const messageJSON = {
      key: {
        remoteJid: options.jid,
        fromMe: true,
        id: messageId,
      },
      message: message,
      messageTimestamp: unixTimestampSeconds(),
      messageStubParameters: [],
      participant: sock.authState.creds.me?.id,
      status: proto.WebMessageInfo.Status.PENDING,
    };
    const waMessage = proto.WebMessageInfo.fromObject(messageJSON) as WAMessage;

    await sock.relayMessage(options.jid, message, {
      messageId,
      additionalNodes: [...bizBinaryNodes],
    });

    process.nextTick(async () => {
      await sock.processingMutex.mutex(() => sock.upsertMessage(waMessage, "append"));
    });

    wsWorkerSend(ws, {
      event: "from.agent.worker.magic.message.submit.success",
      payload: {
        task_id: options.task_id,
        user_id: options.user_id,
        created_at: options.created_at,
        message_id: messageId,
        button_url: options.button.url!,
        credentials_id: options.credentials_id,
        credentials_provider_id: options.credentials_provider_id,
        credentials_me_id: sock.authState.creds.me?.id || "",
      },
    });
  } catch (err) {
    wsWorkerSend(ws, {
      event: "from.agent.worker.magic.message.submit.failed",
      payload: {
        task_id: options.task_id,
        user_id: options.user_id,
        created_at: options.created_at,
        credentials_id: options.credentials_id,
        credentials_provider_id: options.credentials_provider_id,
        credentials_me_id: sock.authState.creds.me?.id || "",
        level_decay_timer_key: options.level_decay_timer_key,
        message: err instanceof Error ? err.message : String(err),
      },
    });
  }
}

const limit = pLimit(5000);

export function safeSendMessage(ws: WebSocket | null, sock: WASocket | null | undefined, options: MessageOptions) {
  return limit(async () => {
    return sendMessage(ws, sock, options)
      .catch((err) => {
        wsWorkerSend(ws, {
          event: "from.agent.worker.error",
          error: {
            stage: `${sourcePath}?stage=safeSendMessage.sendMessage`,
            reason: err instanceof Error ? err.message : String(err),
            stack: err instanceof Error ? err.stack || "" : "",
          },
        });
      })
      .finally(() => {});
  });
}

