import firebase from 'firebase/app'
import firestore, { Firestore } from 'firebase/firestore'

const FIREBASE_EMULATOR_HOST: string = process.env.FIREBASE_EMULATOR_HOST || 'localhost'
const FIRESTORE_EMULATOR_HOST: number = parseInt(process.env.FIRESTORE_EMULATOR_HOST || '8081')

class FirestoreClient {

    private firestore: Firestore

    constructor() {
        this.firestore = firestore.initializeFirestore(firebase.getApp(), {})
        if (process.env.NODE_ENV === 'development') {
            firestore.connectFirestoreEmulator(this.firestore, FIREBASE_EMULATOR_HOST, FIRESTORE_EMULATOR_HOST)
        }
    }

    public addDoc(path: string, data: {}) {
        return firestore.addDoc(firestore.collection(this.firestore, path), data).catch(error => {
            console.error(error)
        })
    }

    public getDocs(path: string) {
        firestore.getDocs(firestore.collection(this.firestore, path)).then(snapshot => {
            return snapshot.docs
        }).catch(error => {
            console.error(error)
        });
    }
}

export const firestoreClient = new FirestoreClient()