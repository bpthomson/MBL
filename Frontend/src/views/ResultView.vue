<script setup>
import { computed } from 'vue'
import { useRoute } from 'vue-router'

const route = useRoute()
const userId = computed(() => route.params.userId || '')
const downloadUrl = computed(() => `/api/download/xml/${encodeURIComponent(userId.value)}`)
</script>

<template>
  <div class="flex-center">
    <div class="wrapper" style="text-align: center;">
      <div class="header-section col">
          <h1 class="text-success">XML Export Complete</h1>
          <p class="sub-text">Payload ready for MAL.</p>
      </div>
      
      <a :href="downloadUrl" class="btn btn-primary btn-block">Download XML Payload</a>
      
      <div class="instructions">
          <strong>Instructions:</strong>
          <ol>
              <li>Save the XML file via the button above.</li>
              <li>Go to the <a href="https://myanimelist.net/import.php" target="_blank" class="hyperlink">MAL Import Portal</a>.</li>
              <li>Select <strong>"MyAnimeList Import"</strong> as the format.</li>
              <li>Upload your saved file and execute the import.</li>
          </ol>
      </div>
      
      <router-link :to="`/select/${userId}`" class="btn btn-outline btn-block mt-4">Return to Selection</router-link>
    </div>
  </div>
</template>

<style scoped>
.instructions { text-align: left; background: #0d1117; padding: 15px; border-radius: 6px; margin: 20px 0; font-size: 13px; color: #c9d1d9; border: 1px solid #30363d; }
.instructions ol { padding-left: 20px; margin: 5px 0; }
.instructions li { margin-bottom: 8px; }
.hyperlink { color: #58a6ff; text-decoration: none; }
.hyperlink:hover { text-decoration: underline; }
.mt-4 { margin-top: 1.5rem; }
</style>
