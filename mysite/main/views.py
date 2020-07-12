from django.shortcuts import render # renders templates
from django.http import HttpResponse
from .models import Tutorial
# Create your views here.

def homepage(request):
    #return HttpResponse('Works')
    return render(request = request,
                  #template being filled
                  template_name = "main/home.html",
                  #context is the information passed to template
                  context = {"tutorials": Tutorial.objects.all})