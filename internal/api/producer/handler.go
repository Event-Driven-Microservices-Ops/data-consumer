package api

type RestHandler struct {
	service *Service
}

func NewRestHandler(s *Service) *RestHandler {
	return &RestHandler{
		service: s,
	}
}
