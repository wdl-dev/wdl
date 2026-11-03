import { WorkerEntrypoint } from "cloudflare:workers";

let received;

export default class extends WorkerEntrypoint {
  open() {
    const state = { text: "", closes: 0 };
    const decoder = new TextDecoder();
    received = state;
    return new WritableStream({
      write(chunk) { state.text += decoder.decode(chunk, { stream: true }); },
      close() {
        state.text += decoder.decode();
        state.closes++;
      },
    });
  }

  state() { return received; }
}
