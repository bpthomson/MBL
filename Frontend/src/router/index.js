import { createRouter, createWebHistory } from 'vue-router'
import HomeView from '../views/HomeView.vue'

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    { path: '/', name: 'home', component: HomeView },
    { path: '/processing', name: 'processing', component: () => import('../views/ProcessingView.vue') },
    { path: '/mal-processing', name: 'mal-processing', component: () => import('../views/MalProcessingView.vue') },
    { path: '/select/:userId', name: 'select', component: () => import('../views/SelectView.vue') },
    { path: '/result/:userId', name: 'result', component: () => import('../views/ResultView.vue') },
    { path: '/music-processing', name: 'music-processing', component: () => import('../views/MusicProcessingView.vue') },
    { path: '/guess-setup', name: 'guess-setup', component: () => import('../views/GuessSetupView.vue') },
    { path: '/guess-processing', name: 'guess-processing', component: () => import('../views/GuessProcessingView.vue') },
    { path: '/guess-game', name: 'guess-game', component: () => import('../views/GuessGameView.vue') },
    { path: '/analytics-processing', name: 'analytics-processing', component: () => import('../views/AnalyticsProcessingView.vue') },
    { path: '/analytics/:userId', name: 'analytics', component: () => import('../views/AnalyticsView.vue') }
  ]
})

export default router
