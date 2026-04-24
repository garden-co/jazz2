import { createApp, h } from "vue";
import { createJazzClient, JazzProvider } from "jazz-tools/vue";
import { BrowserAuthSecretStore } from "jazz-tools";
import App from "./App.vue";

const secret = await BrowserAuthSecretStore.getOrCreateSecret();

const client = createJazzClient({
  appId: import.meta.env.VITE_JAZZ_APP_ID,
  serverUrl: import.meta.env.VITE_JAZZ_SERVER_URL,
  secret,
});

const vueApp = createApp({
  render() {
    return h(
      JazzProvider,
      { client },
      {
        default: () => h(App),
        fallback: () => h("p", "Loading globe..."),
      },
    );
  },
});

vueApp.mount("#app");
