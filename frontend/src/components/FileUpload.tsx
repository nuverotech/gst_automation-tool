'use client'

import { useCallback, useState } from 'react'
import { useDropzone } from 'react-dropzone'
import { Upload, FileSpreadsheet } from 'lucide-react'
import { useFileUpload } from '@/hooks/useFileUpload'
import { MAX_FILE_SIZE, ALLOWED_FILE_TYPES } from '@/utils/constants'

interface FileUploadProps {
  onUploadSuccess: (uploadId: number) => void
  onUploadError: (error: string) => void
}

export default function FileUpload({ onUploadSuccess, onUploadError }: FileUploadProps) {
  const { upload, uploading } = useFileUpload()
  const [selectedFile, setSelectedFile] = useState<File | null>(null)

  const onDrop = useCallback(
    async (acceptedFiles: File[]) => {
      const file = acceptedFiles[0]
      
      if (!file) return

      // Validate file size
      if (file.size > MAX_FILE_SIZE) {
        onUploadError('File size exceeds 10MB limit')
        return
      }

      setSelectedFile(file)

      try {
        const result = await upload(file)
        onUploadSuccess(result.id)
      } catch (error: any) {
        onUploadError(error.message)
      }
    },
    [upload, onUploadSuccess, onUploadError]
  )

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: {
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': ['.xlsx'],
      'application/vnd.ms-excel': ['.xls'],
      'text/csv': ['.csv'],
    },
    maxFiles: 1,
    disabled: uploading,
  })

  return (
    <div className="w-full">
      <div
        {...getRootProps()}
        className={`
          border-2 border-dashed rounded-xl p-12 text-center cursor-pointer
          transition-all duration-200 ease-in-out
          ${isDragActive ? 'border-primary-500 bg-primary-50' : 'border-gray-300 hover:border-primary-400'}
          ${uploading ? 'opacity-50 cursor-not-allowed' : ''}
        `}
      >
        <input {...getInputProps()} />
        
        <div className="flex flex-col items-center justify-center space-y-4">
          {uploading ? (
            <>
              <div className="animate-spin rounded-full h-16 w-16 border-b-2 border-primary-600"></div>
              <p className="text-lg font-medium text-gray-700">Uploading...</p>
              {selectedFile && (
                <p className="text-sm text-gray-500">{selectedFile.name}</p>
              )}
            </>
          ) : (
            <>
              {isDragActive ? (
                <FileSpreadsheet className="w-16 h-16 text-primary-500" />
              ) : (
                <Upload className="w-16 h-16 text-gray-400" />
              )}
              
              <div>
                <p className="text-lg font-medium text-gray-700">
                  {isDragActive ? 'Drop your file here' : 'Drag & drop your Excel file here'}
                </p>
                <p className="text-sm text-gray-500 mt-2">
                  or click to browse
                </p>
              </div>

              <div className="text-xs text-gray-400 space-y-1">
                <p>Supported formats: XLSX, XLS, CSV</p>
                <p>Maximum file size: 50MB</p>
              </div>
            </>
          )}
        </div>
      </div>

      {selectedFile && !uploading && (
        <div className="mt-4 p-4 bg-gray-50 rounded-lg">
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-3">
              <FileSpreadsheet className="w-5 h-5 text-primary-600" />
              <div>
                <p className="text-sm font-medium text-gray-900">{selectedFile.name}</p>
                <p className="text-xs text-gray-500">
                  {(selectedFile.size / 1024 / 1024).toFixed(2)} MB
                </p>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
