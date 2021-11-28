import { createApp } from 'vue'
import App from './App.vue'
import { CloudStorageClient } from './firebase/clients/CloudStorageClient'

let client = new CloudStorageClient()
createApp(App).mount('#app')
