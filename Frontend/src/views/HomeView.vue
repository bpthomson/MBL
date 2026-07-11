<script setup>
import { ref } from 'vue'
import { useRouter } from 'vue-router'
import { useApi } from '../composables/useApi'

const router = useRouter()
const { upload } = useApi()

const bahaUserId = ref('')
const bahaLimit = ref('')
const malUserId = ref('')
const malFile = ref(null)
const fileNameDisplay = ref('+ Select or drop .xml file')
const isDragging = ref(false)
const errorMessage = ref('')

const handleFileChange = (e) => {
  if (e.target.files.length > 0) {
    malFile.value = e.target.files[0]
    fileNameDisplay.value = e.target.files[0].name
  } else {
    malFile.value = null
    fileNameDisplay.value = '+ Select or drop .xml file'
  }
}

const handleFileDrop = (e) => {
  isDragging.value = false
  if (e.dataTransfer.files.length > 0) {
    malFile.value = e.dataTransfer.files[0]
    fileNameDisplay.value = e.dataTransfer.files[0].name
  }
}

const handleBahaSubmit = () => {
  if (!bahaUserId.value) return
  router.push({ 
    path: '/processing', 
    query: { user_id: bahaUserId.value, limit: bahaLimit.value } 
  })
}

const handleMalSubmit = async () => {
  if (!malFile.value) {
    errorMessage.value = 'ERR: Please select an XML file'
    return
  }
  
  const formData = new FormData()
  formData.append('mal_file', malFile.value)
  formData.append('user_id', malUserId.value)
  
  try {
    const res = await upload('/api/import-mal-xml', formData)
    if (res.ok) {
      router.push({ 
        path: '/mal-processing', 
        query: { user_id: res.user_id } 
      })
    } else {
      errorMessage.value = 'ERR: ' + (res.error || 'Import failed')
    }
  } catch (err) {
    errorMessage.value = 'ERR: ' + err.message
  }
}
</script>

<template>
  <div class="flex-center">
    <div class="home-wrapper">
      <div class="hero-header">
          <h1>MBL</h1>
          <p class="sub-text">MyBahaList</p>
      </div>
      
      <div v-if="errorMessage" class="log-line text-error" style="margin-bottom:30px; border:none; text-align:center; background: rgba(255,123,114,0.1); padding: 12px; border-radius: 6px;">
          {{ errorMessage }}
      </div>
      
      <div class="methods-container">
          <div class="import-module blue">
              <div class="module-badge">METHOD.01</div>
              <h3 class="module-title">
                  <span style="color:#58a6ff;">></span> Bahamut Sync
              </h3>
              <form @submit.prevent="handleBahaSubmit">
                  <input type="text" v-model="bahaUserId" placeholder="Enter Bahamut Account ID" required style="margin-bottom: 15px;">
                  <input type="number" v-model="bahaLimit" placeholder="Data Limit (Optional)" style="margin-bottom: 25px;">
                  <button type="submit" class="btn btn-primary btn-block">Initialize Sequence</button>
              </form>
          </div>

          <div class="divider-vertical">
              <span>OR</span>
          </div>

          <div class="import-module green">
              <div class="module-badge">METHOD.02</div>
              <h3 class="module-title">
                  <span style="color:#3fb950;">></span> MAL XML Import
              </h3>
              <form @submit.prevent="handleMalSubmit">
                  <input type="text" v-model="malUserId" placeholder="Enter Display Name (Optional)" style="margin-bottom: 15px;">
                  
                  <div 
                    class="file-drop-area" 
                    :class="{ 'active': malFile || isDragging }"
                    @dragover.prevent="isDragging = true"
                    @dragleave.prevent="isDragging = false"
                    @drop.prevent="handleFileDrop"
                    @click="$refs.fileInput.click()"
                  >
                      <span class="file-name-display" :style="{ color: malFile ? '#c9d1d9' : '#8b949e' }">{{ fileNameDisplay }}</span>
                      <input type="file" ref="fileInput" @change="handleFileChange" accept=".xml">
                  </div>

                  <button type="submit" class="btn btn-success btn-block">Upload & Parse Data</button>
              </form>
          </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.home-wrapper {
    width: 90%;
    max-width: 850px;
    margin: 40px auto;
}
.hero-header {
    text-align: center;
    margin-bottom: 40px;
}
.hero-header h1 {
    font-size: 2.8em;
    color: #58a6ff; 
    letter-spacing: 3px;
    font-weight: 600;
    margin: 0;
    text-shadow: 0 0 20px rgba(88, 166, 255, 0.2);
}
.hero-header .sub-text {
    font-size: 0.9em;
    letter-spacing: 6px;
    color: #8b949e;
    text-transform: uppercase;
}

.methods-container {
    display: flex;
    gap: 30px;
    align-items: stretch;
    justify-content: center;
}

.import-module {
    flex: 1;
    background: #0d1117;
    border: 1px solid #30363d;
    border-radius: 12px;
    padding: 30px 25px;
    transition: all 0.3s ease;
    position: relative;
    overflow: hidden;
    box-shadow: 0 4px 12px rgba(0,0,0,0.2);
    display: flex;
    flex-direction: column;
}
.import-module:hover {
    border-color: #8b949e;
    transform: translateY(-2px);
    box-shadow: 0 8px 20px rgba(0,0,0,0.4);
}
.import-module.blue:hover { border-color: #58a6ff; }
.import-module.green:hover { border-color: #3fb950; }

.import-module form {
    display: flex;
    flex-direction: column;
    flex-grow: 1;
}
.import-module .btn {
    margin-top: auto;
}

.module-badge {
    position: absolute;
    top: 0; right: 0;
    background: #21262d;
    color: #8b949e;
    font-size: 10px;
    padding: 5px 12px;
    border-bottom-left-radius: 8px;
    font-family: monospace;
    letter-spacing: 1px;
}
.module-title {
    margin-top: 0;
    color: #c9d1d9;
    font-size: 16px;
    margin-bottom: 25px;
    display: flex;
    align-items: center;
    gap: 10px;
    font-weight: 500;
}

.file-drop-area {
    position: relative;
    display: flex;
    align-items: center;
    justify-content: center;
    width: 100%;
    height: 50px;
    border: 1px dashed #30363d;
    border-radius: 6px;
    background: #161b22;
    color: #8b949e;
    font-size: 13px;
    font-family: monospace;
    transition: 0.2s;
    cursor: pointer;
    margin-bottom: 20px;
    box-sizing: border-box;
    text-align: center;
}
.file-drop-area:hover, .file-drop-area.active {
    border-color: #3fb950;
    color: #3fb950;
    background: rgba(63, 185, 80, 0.05);
}
.file-drop-area input[type="file"] {
    position: absolute;
    width: 100%;
    height: 100%;
    opacity: 0;
    cursor: pointer;
    top: 0; left: 0;
}
.file-name-display {
    pointer-events: none;
    padding: 0 10px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}

.divider-vertical {
    display: flex;
    align-items: center;
    justify-content: center;
    position: relative;
    width: 40px;
}
.divider-vertical::before {
    content: "";
    position: absolute;
    top: 10%; bottom: 10%; left: 50%;
    width: 1px;
    background: #30363d;
    transform: translateX(-50%);
    z-index: 0;
}
.divider-vertical span {
    background: #0d1117;
    color: #8b949e;
    font-family: monospace;
    font-size: 11px;
    padding: 15px 0;
    position: relative;
    z-index: 1;
}

@media (max-width: 768px) {
    .methods-container {
        flex-direction: column;
    }
    .divider-vertical {
        width: 100%;
        height: 40px;
    }
    .divider-vertical::before {
        top: 50%; bottom: auto; left: 10%; right: 10%;
        width: auto; height: 1px;
        transform: translateY(-50%);
    }
    .divider-vertical span {
        padding: 0 15px;
    }
}
</style>
