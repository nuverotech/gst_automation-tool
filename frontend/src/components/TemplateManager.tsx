'use client'

import { useState, useEffect } from 'react'
import { Upload, Download, Trash2, FileSpreadsheet, Check } from 'lucide-react'
import api from '@/services/api'

export default function TemplateManager() {
  const [templateInfo, setTemplateInfo] = useState<any>(null)
  const [uploading, setUploading] = useState(false)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    fetchTemplateInfo()
  }, [])

  const fetchTemplateInfo = async () => {
    try {
      const response = await api.get('/api/v1/template/current')
      setTemplateInfo(response.data.data)
    } catch (error) {
      console.error('Error fetching template info:', error)
    } finally {
      setLoading(false)
    }
  }

  const handleUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file) return

    setUploading(true)
    const formData = new FormData()
    formData.append('file', file)

    try {
      await api.post('/api/v1/template/upload', formData, {
        headers: { 'Content-Type': 'multipart/form-data' }
      })
      alert('Custom template uploaded successfully!')
      fetchTemplateInfo()
    } catch (error: any) {
      alert(error.response?.data?.detail || 'Upload failed')
    } finally {
      setUploading(false)
    }
  }

  const handleDelete = async () => {
    if (!confirm('Are you sure you want to delete your custom template?')) return

    try {
      await api.delete('/api/v1/template/')
      alert('Custom template deleted. Using default template.')
      fetchTemplateInfo()
    } catch (error: any) {
      alert('Failed to delete template')
    }
  }

  const handleDownloadDefault = async () => {
    try {
      const response = await api.get('/api/v1/template/download-default', {
        responseType: 'blob'
      })
      const url = window.URL.createObjectURL(response.data)
      const link = document.createElement('a')
      link.href = url
      link.download = 'GST_Template_Default.xlsx'
      document.body.appendChild(link)
      link.click()
      document.body.removeChild(link)
      window.URL.revokeObjectURL(url)
    } catch (error) {
      alert('Failed to download default template')
    }
  }

  if (loading) return <div>Loading...</div>

  return (
    <div className="bg-white rounded-xl shadow-lg p-6">
      <h2 className="text-xl font-semibold text-gray-900 mb-4">Template Settings</h2>
      
      <div className="space-y-4">
        {/* Current Template */}
        <div className="flex items-center justify-between p-4 bg-gray-50 rounded-lg">
          <div className="flex items-center space-x-3">
            <FileSpreadsheet className="w-6 h-6 text-primary-600" />
            <div>
              <p className="font-medium text-gray-900">{templateInfo?.template_name}</p>
              <p className="text-sm text-gray-600">
                {templateInfo?.is_custom ? 'Custom uploaded template' : 'System default template'}
              </p>
            </div>
          </div>
          {templateInfo?.is_custom && (
            <Check className="w-5 h-5 text-green-600" />
          )}
        </div>

        {/* Actions */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {/* Upload Custom */}
          <label className="flex flex-col items-center justify-center p-4 border-2 border-dashed border-gray-300 rounded-lg cursor-pointer hover:border-primary-500 transition-colors">
            <Upload className="w-8 h-8 text-gray-400 mb-2" />
            <span className="text-sm font-medium text-gray-700">Upload Custom</span>
            <input
              type="file"
              accept=".xlsx,.xls"
              onChange={handleUpload}
              disabled={uploading}
              className="hidden"
            />
          </label>

          {/* Download Default */}
          <button
            onClick={handleDownloadDefault}
            className="flex flex-col items-center justify-center p-4 border-2 border-gray-300 rounded-lg hover:border-primary-500 transition-colors"
          >
            <Download className="w-8 h-8 text-gray-400 mb-2" />
            <span className="text-sm font-medium text-gray-700">Download Default</span>
          </button>

          {/* Delete Custom */}
          {templateInfo?.can_delete && (
            <button
              onClick={handleDelete}
              className="flex flex-col items-center justify-center p-4 border-2 border-red-300 rounded-lg hover:border-red-500 transition-colors"
            >
              <Trash2 className="w-8 h-8 text-red-400 mb-2" />
              <span className="text-sm font-medium text-red-700">Delete Custom</span>
            </button>
          )}
        </div>

        <p className="text-xs text-gray-500 mt-4">
          Upload your own Excel template to customize the output format. The system will map your data to match your template's structure.
        </p>
      </div>
    </div>
  )
}
