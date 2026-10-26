import http from './http'

export const getRecommendations = () => http.get('/recommendations')
export const getPublicRecommendations = () => http.get('/recommendations/public')
export const refreshRecommendations = () => http.post('/recommendations/refresh')
