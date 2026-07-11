<script setup>
defineProps({
  userId: { type: String, default: '' },
  showErrorLogs: { type: Boolean, default: true },
  showTerminate: { type: Boolean, default: false },
  terminateUrl: { type: String, default: '/' }
})
</script>

<template>
  <div class="navbar">
    <router-link to="/" class="navbar-brand">
        <span style="color: #005bb5; font-weight: bold; margin-right: 5px;">/></span>MBL System
    </router-link>
    
    <div class="navbar-links" style="display: flex; align-items: center; gap: 15px;">
        <span style="display: flex; align-items: center; gap: 5px; color: #3fb950; font-family: monospace; font-size: 11px;">
            <span style="width: 6px; height: 6px; background-color: #3fb950; border-radius: 50%; display: inline-block; box-shadow: 0 0 5px #3fb950;"></span>
            SYS.ONLINE
        </span>
        <span v-if="userId" style="color: #8b949e; font-family: monospace; font-size: 11px; border-left: 1px solid #30363d; padding-left: 15px;">
            TARGET : <span style="color: #c9d1d9;">{{ userId }}</span>
        </span>
        <span style="border-left: 1px solid #30363d; padding-left: 15px;">
            <a href="https://docs.google.com/spreadsheets/d/14lacBmNveOxbD461wObB9x0KW86dFY4GgsTbte8iOo8/edit" target="_blank" v-if="showErrorLogs">[ Error Logs ]</a>
        </span>
    </div>
  </div>
</template>

<style scoped>
/* Scoped styles are left intentionally empty as we rely on the global style.css for navbar styling which perfectly matches the original */
</style>
