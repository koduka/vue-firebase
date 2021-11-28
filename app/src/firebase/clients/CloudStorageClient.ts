import firebaseApp from 'firebase/app'
import storage, { UploadMetadata } from 'firebase/storage'

const FIREBASE_EMULATOR_HOST: string = process.env.FIREBASE_EMULATOR_HOST || 'localhost'
const FIREBASE_STORAGE_EMULATOR_PORT: number = parseInt(process.env.FIREBASE_STORAGE_EMULATOR_PORT || '9000')

export class CloudStorageClient {

    constructor(bukectUrl?: string) {
        if (process.env.NODE_ENV === 'development') {
            storage.connectStorageEmulator(this.getStorage(bukectUrl), FIREBASE_EMULATOR_HOST, FIREBASE_STORAGE_EMULATOR_PORT)
        }
    }

    public getStorage(bucketUrl?: string) {
        return storage.getStorage(firebaseApp.getApp(), bucketUrl)
    }

    public upload(data: Blob | Uint8Array | ArrayBuffer, contentType: string, bukectUrl?: string) {
        const metadata: UploadMetadata = {
            contentType: contentType
        }
        return storage.uploadBytes(storage.ref(this.getStorage(bukectUrl)), data, metadata).catch(error => {
            console.error(error)
        })
    }

    public getStream(path: string, bukectUrl?: string) {
        return storage.getStream(storage.ref(this.getStorage(bukectUrl), path), 100 * 1024 * 1024)
    }

    public getDowloadUrl(path: string, bukectUrl?: string) {
        return storage.getDownloadURL(storage.ref(this.getStorage(bukectUrl), path)).catch(error => {
            console.error(error)
        })
    }
}
