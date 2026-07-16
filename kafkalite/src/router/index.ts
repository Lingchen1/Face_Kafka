import { createRouter, createWebHashHistory } from "vue-router";
import ConnectionsView from "../views/ConnectionsView.vue";
import TopicsView from "../views/TopicsView.vue";
import MessagesView from "../views/MessagesView.vue";

export const router = createRouter({
  history: createWebHashHistory(),
  routes: [
    { path: "/", redirect: "/connections" },
    { path: "/connections", component: ConnectionsView },
    { path: "/topics", component: TopicsView },
    { path: "/messages", component: MessagesView },
  ],
});
