#!/usr/bin/python
""" web-interface for pybusta """
# -*- coding: utf-8 -*-
from wsgiref.simple_server import make_server
from pyramid.config import Configurator
from pyramid.response import Response
from pyramid.response import FileResponse
from pyramid.renderers import render_to_response

import lib.bookindex
import urllib
import os

class WebApp(object):
    """ container class for web application """
    def __init__(self):
        self.book_index = lib.bookindex.BookIndex()
        self.template_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 'templates')
    def search(self, request):
        """ perform search and return results """
        search_query = {}
        output = {}
        output['items'] = []
        try:
            search_query['author'] =\
                urllib.unquote(request.matchdict['name']).upper()
        except:
            pass
        try:
            search_query['title'] =\
                urllib.unquote(request.matchdict['title']).upper()
        except:
            pass
        for row  in self.book_index.query_fulltext_index(
                search_query,
                response_type=dict):
            output['items'].append(row)
        return render_to_response(
            os.path.join(self.template_dir, 'search_results.mak'), output,
            request=request)
    def search_form(self, request):
        """ render search form """
        return render_to_response(os.path.join(self.template_dir, 'search_form.mak'), {}, request=request)
    def search_query_parse(self, request):    
        """ parsing args from search form and redirecting to search url """
        author = None
        title = None
        search_url = None
        base_url = "/search"
        try:
            author = request.GET['author'].encode('utf-8')
        except: pass    
        try:
            title = request.GET['title'].encode('utf-8')
        except: pass    
        if author:
            if title:
                search_url = base_url+"/author/"+author+"/title/"+title
            else:
                search_url = base_url+"/author/"+author
        elif title:
            search_url = base_url+"/title/"+title
        else:
            search_url = "/"
        return Response(status_int=302, location=search_url)
    def get_book(self, request):
        """ extracting book by id and redirecting to download url """
        try:
            book_id = int(urllib.unquote(request.matchdict['id']).upper())
        except: 
            book_id = None
        file_name = self.book_index.extract_book(book_id)
        return Response(
            status_int=302,
            location="/download/%s" % file_name.encode('utf-8')
            )
    def download_book(self, request):
        """ perform download by filename """
        file_name = urllib.unquote(request.matchdict['file_name'])
        file_path = os.path.join(
            self.book_index.config['extractPath'],
            file_name)
        return FileResponse(path=file_path)


if __name__ == '__main__':
    ap = WebApp()
    config = Configurator()
    config.include('pyramid_mako')
    config.add_route('search_form', '/')
    config.add_route('search_query_parse', '/booksearch')
    config.add_route('get_book', '/get/{id}')
    config.add_route('download_book', '/download/{file_name}')
    config.add_route('search_author', '/search/author/{name}')
    config.add_route('search_title', '/search/title/{title}')
    config.add_route(
        'search_author_title',
        '/search/author/{name}/title/{title}')
    config.add_view(ap.search_form, route_name='search_form')
    config.add_view(ap.search, route_name='search_author')
    config.add_view(ap.search, route_name='search_title')
    config.add_view(ap.search, route_name='search_author_title')
    config.add_view(ap.get_book, route_name='get_book')
    config.add_view(ap.download_book, route_name='download_book')
    config.add_view(ap.search_query_parse, route_name='search_query_parse')
    app = config.make_wsgi_app()
    server = make_server('0.0.0.0', 8080, app)
    server.serve_forever()
