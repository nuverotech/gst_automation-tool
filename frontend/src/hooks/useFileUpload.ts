import { useState } from 'react'
import { uploadFile } from '@/services/api'

export const useFileUpload = () => {
  const [uploading, setUploading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const upload = async (file: File) => {
    setUploading(true)
    setError(null)

    try {
      const response = await uploadFile(file)
      
      if (response.success && response.data) {
        return response.data
      } else {
        throw new Error(response.error || 'Upload failed')
      }
    } catch (err: any) {
      const errorMessage = err.response?.data?.error || err.message || 'Upload failed'
      setError(errorMessage)
      throw new Error(errorMessage)
    } finally {
      setUploading(false)
    }
  }

  return {
    upload,
    uploading,
    error,
  }
}
