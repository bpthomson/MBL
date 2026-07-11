<script setup>
import { computed } from 'vue'
import { useRoute } from 'vue-router'
import AppNavbar from './components/AppNavbar.vue'

const route = useRoute()
const userId = computed(() => route.params.userId || route.query.user_id || '')
</script>

<template>
  <div class="app-container">
    <AppNavbar :userId="userId" :showTerminate="true" />
    <router-view />
  </div>
</template>
